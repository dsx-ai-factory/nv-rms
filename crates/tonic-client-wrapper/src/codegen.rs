/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-Apache-2.0
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro2::{LexError, TokenStream};
use prost_types::field_descriptor_proto::Label;
use prost_types::{FileDescriptorProto, MethodDescriptorProto};
use quote::{TokenStreamExt, quote};

use crate::utils::{base_types, field_is_optional, resolve_field_primitive_type};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid token for {target}: {error}")]
    InvalidToken {
        target: String,
        error: proc_macro2::LexError,
    },
    #[error(transparent)]
    Lex(#[from] LexError),
    #[error("Invalid protobuf type: {0}")]
    InvalidProtobufType(String),

    /// The requested input proto could not be matched to its descriptor.
    #[error("Input protobuf file was not present in the descriptor set: {0}")]
    MissingProtoFile(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Syntax error in generated code: {0}")]
    Syntax(#[from] syn::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Configures code generation of the tonic client wrapper
pub struct Config {
    /// The name of the generated tonic client wrapper
    pub wrapper_name: String,
    /// The fully qualified type of the tonic client this wrapper will be calling
    pub inner_rpc_client_type: String,

    /// The module path of the generated types within your crate, not including the service name,
    /// relative to the crate root (do not include `crate::`.) This is used to fully qualify the
    /// type names in the generated wrapper methods.
    ///
    /// For example, if your service `my_service` is generated in `crate::rpc::protos::my_service`,
    /// use `"rpc::protos"` here.
    pub generated_types_path_within_crate: String,

    /// The input protobuf files to generate wrappers from
    pub proto_files: Vec<String>,

    /// Include paths for types referenced by `proto_files`
    pub include_paths: Vec<String>,

    /// List of protobuf types to override with specific rust types. This should mirror any types you are customizing via [`tonic_prost_build::Builder::extern_path`] to make the generated code match.
    pub extern_paths: Vec<(ProtobufType, RustType)>,
}

pub type ProtobufType = &'static str;
pub type RustType = &'static str;

pub struct CodeGenerator {
    inner_rpc_client_type: TokenStream,
    wrapper_name: TokenStream,

    /// Only the explicitly listed proto files, used for service/converter generation.
    /// Transitive imports are excluded; cross-file type references are resolved via message_types.
    target_proto_fds: Vec<FileDescriptorProto>,
    generated_types_path_within_crate: TokenStream,
    message_types: HashMap<String, MessageWithPackage>,
    extern_paths: HashMap<ProtobufType, RustType>,
}

impl CodeGenerator {
    pub fn new(config: Config) -> Result<Self> {
        let inner_rpc_client_type =
            config
                .inner_rpc_client_type
                .parse()
                .map_err(|error| Error::InvalidToken {
                    target: "inner_rpc_client_type".to_string(),
                    error,
                })?;
        let wrapper_name = config
            .wrapper_name
            .parse()
            .map_err(|error| Error::InvalidToken {
                target: "wrapper_name".to_string(),
                error,
            })?;
        let generated_types_path_within_crate = config
            .generated_types_path_within_crate
            .parse()
            .map_err(|error| Error::InvalidToken {
                target: "generated_types_path_within_crate".to_string(),
                error,
            })?;

        let all_proto_fds = tonic_prost_build::Config::new()
            .protoc_arg("--experimental_allow_proto3_optional")
            .load_fds(
                config.proto_files.as_slice(),
                config.include_paths.as_slice(),
            )?
            .file;

        // Descriptor names are relative to an include path and may retain nested directories.
        // Resolve each input to the longest matching descriptor suffix so imported services stay
        // excluded without losing explicitly listed nested proto files.
        let target_filenames: HashSet<String> = config
            .proto_files
            .iter()
            .map(|proto_file| {
                let proto_path =
                    fs::canonicalize(proto_file).unwrap_or_else(|_| PathBuf::from(proto_file));

                all_proto_fds
                    .iter()
                    .filter_map(|fd| fd.name.as_deref())
                    .filter(|name| proto_path.ends_with(Path::new(name)))
                    .max_by_key(|name| Path::new(name).components().count())
                    .map(str::to_string)
                    .ok_or_else(|| Error::MissingProtoFile(proto_file.clone()))
            })
            .collect::<Result<_>>()?;

        let target_proto_fds: Vec<FileDescriptorProto> = all_proto_fds
            .iter()
            .filter(|fd| {
                fd.name
                    .as_deref()
                    .map(|name| target_filenames.contains(name))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        // Make an index of the messages by fully-qualified name, so we can refer to them later.
        // Include transitive imports so cross-file type references resolve correctly.
        let message_types: HashMap<String, MessageWithPackage> = all_proto_fds
            .iter()
            .flat_map(|fd| {
                fd.message_type.iter().map(|message| {
                    let message_with_package = MessageWithPackage {
                        message: message.clone(),
                        package: fd.package.clone(),
                    };
                    (message_with_package.qualified_name(), message_with_package)
                })
            })
            .collect();

        let extern_paths = config.extern_paths.into_iter().collect();

        Ok(Self {
            inner_rpc_client_type,
            wrapper_name,
            target_proto_fds,
            generated_types_path_within_crate,
            message_types,
            extern_paths,
        })
    }

    /// Write the tonic client wrapper out to a file.
    pub fn write_rpc_client_wrapper<P: AsRef<Path>>(&self, out: P) -> Result<()> {
        let mut wrapper_methods = TokenStream::new();

        self.target_proto_fds
            .iter()
            .flat_map(|fd| &fd.service)
            .flat_map(|svc| &svc.method)
            .map(|m| self.make_rpc_wrapper_method(m))
            .collect::<Result<Vec<_>>>()? // fail if any of the wrappers failed
            .into_iter()
            .for_each(|m| wrapper_methods.append_all(m));

        let inner_rpc_client_type = &self.inner_rpc_client_type;
        let wrapper_name = &self.wrapper_name;

        let file = quote! {
            use std::ops::Deref;

            #[derive(Clone, Debug)]
            pub struct #wrapper_name {
                inner: std::sync::Arc<Inner>
            }

            #[derive(Debug)]
            struct Inner {
                connection_provider: Box<dyn ::tonic_client_wrapper::ConnectionProvider<#inner_rpc_client_type>>,
                connection: ::tokio::sync::Mutex<Option<InnerConnection>>,
            }

            #[derive(Debug)]
            struct InnerConnection {
                client: #inner_rpc_client_type,
                created: std::time::SystemTime,
            }

            impl #wrapper_name {
                pub fn build<P: ::tonic_client_wrapper::ConnectionProvider<#inner_rpc_client_type>>(connection_provider: P) -> Self {
                    let inner = Inner {
                        connection_provider: Box::new(connection_provider),
                        connection: tokio::sync::Mutex::new(None),
                    };

                    Self {
                        inner: std::sync::Arc::new(inner),
                    }
                }

                pub async fn connection(&self) -> std::result::Result<#inner_rpc_client_type, tonic::Status> {
                    let mut guard = self.inner.connection.lock().await;
                    if let Some(connection) = guard.deref() {
                        if self.inner.connection_provider.connection_is_stale(connection.created).await {
                            guard.take();
                        }
                    }

                    match guard.deref() {
                        Some(connection) => Ok(connection.client.clone()),
                        None => {
                            let client = self.inner.connection_provider.provide_connection().await?;
                            guard.replace(InnerConnection {
                                client: client.clone(),
                                created: std::time::SystemTime::now(),
                            });
                            Ok(client)
                        }
                    }
                }

                pub fn url(&self) -> &str {
                    self.inner.connection_provider.connection_url()
                }

                #wrapper_methods
            }
        };

        write_token_stream_if_not_up_to_date(file, &out)?;
        Ok(())
    }

    /// Write convience `From<...>` implementations for each type referenced by a gRPC method in the
    /// proto files.
    ///
    /// A converter will be written for a type if:
    ///
    /// - It's used as the input for a gRPC method
    /// - It has zero or one fields.
    ///
    /// If the type has zero fields, a converter will be generated from the empty tuple (`()`).
    ///
    /// If the type has one field, a converter will be generated from any type which is convertible
    /// to that single field (ie. `From<T: Into<SomeField>>`)
    pub fn write_rpc_convenience_converters<P: AsRef<Path>>(&self, out: P) -> Result<()> {
        // Grab the input type of every method from every service in every file. Use a HashSet so we
        // don't create the same converter twice
        let method_inputs_type_strings: HashSet<&String> = self
            .target_proto_fds
            .iter()
            .flat_map(|fd| &fd.service)
            .flat_map(|service| &service.method)
            .filter_map(|method| method.input_type.as_ref())
            .collect();

        let mut converters = TokenStream::new();
        // Look up each input type in self.message_types to get its metadata
        let mut sorted_messages = method_inputs_type_strings
            .into_iter()
            .filter_map(|t| self.message_types.get(t))
            .collect::<Vec<_>>();

        sorted_messages.sort_by(|a, b| match a.package.cmp(&b.package) {
            Ordering::Equal => a.message.name.cmp(&b.message.name),
            other => other,
        });
        for message_and_package in sorted_messages {
            // Generate a convenience converter for each one.
            converters.append_all(self.make_convenience_converter(message_and_package)?);
        }

        write_token_stream_if_not_up_to_date(converters, &out)?;
        Ok(())
    }

    fn make_convenience_converter(
        &self,
        message_with_package: &MessageWithPackage,
    ) -> Result<Option<TokenStream>> {
        let message = &message_with_package.message;
        let qualified_name = message_with_package.qualified_name();

        if message.field.len() > 1 {
            // We only make convenience converters for messages with 1 or 0 fields
            return Ok(None);
        }

        if base_types.contains_key(&qualified_name) {
            // Except we can't create convenience converters for primitives
            return Ok(None);
        }

        if self.extern_paths.contains_key(qualified_name.as_str()) {
            // Nor do we create them for extern types
            return Ok(None);
        }

        // No fields in the message means we can convert from `()`
        let Some(field) = message.field.first() else {
            return Ok(Some(
                self.make_convenience_converter_from_void(message_with_package)?,
            ));
        };

        let is_repeated = field.label.is_some_and(|l| l == Label::Repeated as i32);
        let is_optional = field_is_optional(field);

        // Define the template values used in the generated code...

        // The type of the message itself being converted *to*
        let message_type: TokenStream = self
            .convert_protobuf_type_to_rust_type(&message_with_package.qualified_name())?
            .parse()?;

        // The name of the single field we're going to be populating from the From<> type
        let field_name = if let Some(oneof_index) = field.oneof_index {
            message.oneof_decl[oneof_index as usize].name()
        } else {
            field.name()
        }
        .to_snake_case()
        .parse::<TokenStream>()?;

        // The type of the single field
        let field_type: TokenStream = {
            let type_str = if let Some(t) = resolve_field_primitive_type(field) {
                t
            } else if let Some(type_name) = &field.type_name {
                self.convert_protobuf_type_to_rust_type(type_name)?
            } else {
                // This might be a primitive type we don't know about.
                return Ok(None);
            };

            if is_repeated {
                format!("Vec<{type_str}>").parse()?
            } else {
                type_str.parse()?
            }
        };

        // The value we're setting the single field to
        let value: TokenStream =
            if field.oneof_index.is_some() && field.proto3_optional.is_none_or(|o| !o) {
                // If it's a `oneof`, it's going to be seen by rust as an Enum with an associated
                // value. The enum package is going to be the message name in snake-case, the enum's
                // type is the name of the oneof field, and each arm of the enum is going to be one
                // of the oneof arms (which we've ensured there is only one.)
                //
                // Note about proto3_optional: If proto3_optional is set, it generally means that
                // this is a "synthetic" oneof, which is a sort of hack used by prost-types to make
                // the field show up as optional for proto3. We *don't* want to treat these cases as
                // enums, because they don't show up in rust as real enums.
                self.convert_protobuf_type_to_rust_type(&format!(
                    "{}.{}",
                    message_with_package.qualified_name(), // this will be snake_cased
                    message.oneof_decl[field.oneof_index() as usize].name() // this will be CamelCased
                ))
                .map(|s| {
                    format!(
                        "Some({}::{}(t.into()))",
                        s,
                        field.name().to_upper_camel_case()
                    )
                })?
                .parse()?
            } else if is_optional {
                "Some(t.into())".parse()?
            } else {
                "t.into()".parse()?
            };

        Ok(Some(quote! {
            impl<T: Into<#field_type>> From<T> for #message_type {
                fn from(t: T) -> Self {
                    Self {
                        #field_name: #value
                    }
                }
            }
        }))
    }

    fn make_convenience_converter_from_void(
        &self,
        message_with_package: &MessageWithPackage,
    ) -> Result<TokenStream> {
        let message_type: TokenStream = self
            .convert_protobuf_type_to_rust_type(&message_with_package.qualified_name())?
            .parse()?;

        Ok(quote! {
            impl From<()> for #message_type {
                fn from(_: ()) -> Self {
                    Self {}
                }
            }
        })
    }

    fn make_rpc_wrapper_method(&self, method: &MethodDescriptorProto) -> Result<TokenStream> {
        let method_name: TokenStream = method.name().to_snake_case().parse()?;
        let input_type_str = self.convert_protobuf_type_to_rust_type(method.input_type())?;
        let input_type: TokenStream = input_type_str.parse()?;
        let output_type: TokenStream = self
            .convert_protobuf_type_to_rust_type(method.output_type())?
            .parse()?;

        let is_client_streaming = method.client_streaming.unwrap_or(false);
        let is_server_streaming = method.server_streaming.unwrap_or(false);

        match (is_client_streaming, is_server_streaming) {
            (true, true) => {
                // Bidirectional streaming.
                Ok(quote! {
                    pub async fn #method_name<S>(&self, request: S) -> Result<tonic::Response<tonic::codec::Streaming<#output_type>>, tonic::Status>
                    where
                        S: tonic::IntoStreamingRequest<Message = #input_type>,
                    {
                        self.connection().await?.#method_name(request).await
                    }
                })
            }
            (true, false) => {
                // Client streaming.
                Ok(quote! {
                    pub async fn #method_name<S>(&self, request: S) -> Result<#output_type, tonic::Status>
                    where
                        S: tonic::IntoStreamingRequest<Message = #input_type>,
                    {
                        Ok(self
                            .connection()
                            .await?
                            .#method_name(request)
                            .await?
                            .into_inner())
                    }
                })
            }
            (false, true) => {
                // Server streaming.
                let token_stream = if input_type_str == "()" {
                    quote! {
                        pub async fn #method_name(&self) -> Result<tonic::codec::Streaming<#output_type>, tonic::Status> {
                            Ok(self
                                .connection()
                                .await?
                                .#method_name(tonic::Request::new(()))
                                .await?
                                .into_inner())
                        }
                    }
                } else {
                    let has_zero_fields = method
                        .input_type
                        .as_ref()
                        .and_then(|t| self.message_types.get(t))
                        .is_some_and(|t| t.message.field.is_empty());

                    if has_zero_fields {
                        quote! {
                            pub async fn #method_name(&self) -> Result<tonic::codec::Streaming<#output_type>, tonic::Status> {
                                Ok(self
                                    .connection()
                                    .await?
                                    .#method_name(tonic::Request::new(#input_type {}))
                                    .await?
                                    .into_inner())
                            }
                        }
                    } else {
                        quote! {
                            pub async fn #method_name<T: Into<#input_type>>(&self, request: T) -> Result<tonic::codec::Streaming<#output_type>, tonic::Status> {
                                Ok(self
                                    .connection()
                                    .await?
                                    .#method_name(tonic::Request::new(request.into()))
                                    .await?
                                    .into_inner())
                            }
                        }
                    }
                };
                Ok(token_stream)
            }
            (false, false) => {
                let method_label = method.name();
                let request_type = method.input_type().trim_start_matches('.');
                let response_type = method.output_type().trim_start_matches('.');
                let has_zero_fields = method
                    .input_type
                    .as_ref()
                    .and_then(|t| self.message_types.get(t))
                    .is_some_and(|t| t.message.field.is_empty());
                let (signature, request) = if input_type_str == "()" {
                    (
                        quote! { pub async fn #method_name(&self) -> Result<#output_type, tonic::Status> },
                        quote! { () },
                    )
                } else if has_zero_fields {
                    (
                        quote! { pub async fn #method_name(&self) -> Result<#output_type, tonic::Status> },
                        quote! { #input_type {} },
                    )
                } else {
                    (
                        quote! { pub async fn #method_name<T: Into<#input_type>>(&self, request: T) -> Result<#output_type, tonic::Status> },
                        quote! { request.into() },
                    )
                };
                Ok(quote! {
                    #signature {
                        let request: #input_type = #request;
                        let observation = ::tonic_client_wrapper::start_rpc(
                            self.inner.connection_provider.rpc_observer(),
                            #method_label, #request_type, &request,
                        );
                        ::tonic_client_wrapper::finish_rpc(observation, #response_type, async {
                            Ok(self.connection().await?
                                .#method_name(tonic::Request::new(request)).await?.into_inner())
                        }, tonic::Status::code).await
                    }
                })
            }
        }
    }

    /// Convert tye protobuf type (which looks like `.forge.VersionRequest` or similar) to the proper
    /// rust type, by:
    ///
    /// - Converting it to a known base type (bool, (), etc) if it's a known base type
    ///
    /// or:
    ///
    /// - Stripping the leading `.`
    /// - Converting all but the last dot-separated components into snake_case
    /// - Converting the last dot-separated component into CamelCase
    /// - Joining the components with `::` instead of `.`
    /// - Prefixing the type with `crate::<generated_types_path_within_crate>::`, to make it a fully
    ///   qualified path.
    pub(crate) fn convert_protobuf_type_to_rust_type(&self, t: &str) -> Result<String> {
        if let Some(base_type) = base_types.get(t) {
            return Ok(base_type.to_owned());
        }

        if let Some(extern_type) = self.extern_paths.get(t) {
            return Ok(extern_type.to_string());
        }

        let components = t
            .strip_prefix(".")
            .ok_or_else(|| Error::InvalidProtobufType(t.to_string()))?
            .split('.')
            .collect::<Vec<_>>();
        let result = if components.len() > 1 {
            let leading = components[0..components.len() - 1]
                .iter()
                .map(|s| s.to_snake_case())
                .collect::<Vec<_>>()
                .join("::");
            let last = components[components.len() - 1].to_upper_camel_case();
            [leading, last].join("::")
        } else if let Some(last_component) = components.last() {
            last_component.to_upper_camel_case()
        } else {
            return Err(Error::InvalidProtobufType(t.to_string()));
        };

        Ok(format!(
            "crate::{}::{}",
            self.generated_types_path_within_crate, result
        ))
    }
}

#[derive(Debug)]
struct MessageWithPackage {
    package: Option<String>,
    message: prost_types::DescriptorProto,
}

impl MessageWithPackage {
    fn qualified_name(&self) -> String {
        if let Some(package) = &self.package {
            format!(".{}.{}", package, self.message.name())
        } else {
            format!(".{}", self.message.name())
        }
    }
}

fn write_token_stream_if_not_up_to_date<T: AsRef<Path>>(
    token_stream: TokenStream,
    out: T,
) -> Result<()> {
    let ast: syn::File = syn::parse2(token_stream)?;
    let code = prettyplease::unparse(&ast);

    let up_to_date = match fs::read_to_string(&out) {
        Ok(existing) => code == existing,
        Err(_) => false,
    };

    if !up_to_date {
        fs::write(&out, code)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use prost_types::MethodDescriptorProto;
    use quote::quote;

    use super::*;

    fn test_generator() -> CodeGenerator {
        let proto_file =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/test_fixtures/test.proto");
        let include_path = proto_file
            .parent()
            .expect("test fixture path should have a parent");
        let cfg = Config {
            wrapper_name: "TestWrapper".to_string(),
            inner_rpc_client_type: "TestInnerClient".to_string(),
            generated_types_path_within_crate: "protos".to_string(),
            proto_files: vec![proto_file.to_string_lossy().to_string()],
            include_paths: vec![include_path.to_string_lossy().to_string()],
            extern_paths: vec![(".test.ExternType", "crate::CustomExternType")],
        };

        CodeGenerator::new(cfg).expect("test generator should parse fixture proto")
    }

    fn rpc_methods(generator: &CodeGenerator) -> HashMap<&str, &MethodDescriptorProto> {
        generator
            .target_proto_fds
            .iter()
            .flat_map(|file| &file.service)
            .flat_map(|service| &service.method)
            .map(|method| (method.name(), method))
            .collect()
    }

    fn message<'a>(generator: &'a CodeGenerator, name: &str) -> &'a MessageWithPackage {
        generator
            .message_types
            .get(name)
            .unwrap_or_else(|| panic!("missing test message {name}"))
    }

    fn assert_tokens_eq(name: &str, actual: TokenStream, expected: TokenStream) {
        assert_eq!(actual.to_string(), expected.to_string(), "{name}");
    }

    #[test]
    fn includes_rpc_methods_from_nested_target_proto() {
        let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/test_fixtures");

        let proto_file = fixture_root.join("nested/test.proto");

        let cfg = Config {
            wrapper_name: "NestedTestWrapper".to_string(),
            inner_rpc_client_type: "NestedTestInnerClient".to_string(),
            generated_types_path_within_crate: "protos".to_string(),
            proto_files: vec![proto_file.to_string_lossy().to_string()],
            include_paths: vec![fixture_root.to_string_lossy().to_string()],
            extern_paths: vec![],
        };

        let generator = CodeGenerator::new(cfg).expect("nested target fixture should load");

        let methods = rpc_methods(&generator);

        assert!(
            methods.contains_key("Ping"),
            "nested target RPC was omitted"
        );
    }

    #[test]
    fn generates_rpc_wrapper_methods() {
        struct TestCase {
            name: &'static str,
            expected: TokenStream,
        }

        let generator = test_generator();
        let methods = rpc_methods(&generator);
        let cases = [
            TestCase {
                name: "VoidRpc",
                expected: quote! {
                    pub async fn void_rpc(&self) -> Result<crate::protos::test::SomeResponse, tonic::Status> {
                        let request: crate::protos::test::VoidRequest = crate::protos::test::VoidRequest {};
                        let observation = ::tonic_client_wrapper::start_rpc(
                            self.inner.connection_provider.rpc_observer(), "VoidRpc", "test.VoidRequest", &request,
                        );
                        ::tonic_client_wrapper::finish_rpc(observation, "test.SomeResponse", async {
                            Ok(self.connection().await?.void_rpc(tonic::Request::new(request)).await?.into_inner())
                        }, tonic::Status::code).await
                    }
                },
            },
            TestCase {
                name: "SingleMessageRpc",
                expected: quote! {
                    pub async fn single_message_rpc<T: Into<crate::protos::test::SingleMessageRequest>>(&self, request: T) -> Result<crate::protos::test::SomeResponse, tonic::Status> {
                        let request: crate::protos::test::SingleMessageRequest = request.into();
                        let observation = ::tonic_client_wrapper::start_rpc(
                            self.inner.connection_provider.rpc_observer(), "SingleMessageRpc", "test.SingleMessageRequest", &request,
                        );
                        ::tonic_client_wrapper::finish_rpc(observation, "test.SomeResponse", async {
                            Ok(self.connection().await?.single_message_rpc(tonic::Request::new(request)).await?.into_inner())
                        }, tonic::Status::code).await
                    }
                },
            },
            TestCase {
                name: "SingleStreamingMessageRpc",
                expected: quote! {
                    pub async fn single_streaming_message_rpc<T: Into<crate::protos::test::SingleMessageRequest>>(&self, request: T) -> Result<tonic::codec::Streaming<crate::protos::test::SomeResponse>, tonic::Status> {
                        Ok(self
                            .connection()
                            .await?
                            .single_streaming_message_rpc(tonic::Request::new(request.into()))
                            .await?
                            .into_inner())
                    }
                },
            },
            TestCase {
                name: "ClientStreamingRpc",
                expected: quote! {
                    pub async fn client_streaming_rpc<S>(&self, request: S) -> Result<crate::protos::test::SomeResponse, tonic::Status>
                    where
                        S: tonic::IntoStreamingRequest<Message = crate::protos::test::SingleMessageRequest>,
                    {
                        Ok(self
                            .connection()
                            .await?
                            .client_streaming_rpc(request)
                            .await?
                            .into_inner())
                    }
                },
            },
            TestCase {
                name: "BidirectionalStreamingRpc",
                expected: quote! {
                    pub async fn bidirectional_streaming_rpc<S>(&self, request: S) -> Result<tonic::Response<tonic::codec::Streaming<crate::protos::test::SomeResponse>>, tonic::Status>
                    where
                        S: tonic::IntoStreamingRequest<Message = crate::protos::test::SingleMessageRequest>,
                    {
                        self.connection().await?.bidirectional_streaming_rpc(request).await
                    }
                },
            },
        ];

        for case in cases {
            let method = methods
                .get(case.name)
                .unwrap_or_else(|| panic!("missing test RPC {}", case.name));
            let actual = generator
                .make_rpc_wrapper_method(method)
                .unwrap_or_else(|error| panic!("failed to generate {}: {error}", case.name));

            assert_tokens_eq(case.name, actual, case.expected);
        }
    }

    #[test]
    fn generates_convenience_converters() {
        struct TestCase {
            name: &'static str,
            expected: TokenStream,
        }

        let generator = test_generator();
        let cases = [
            TestCase {
                name: ".test.VoidRequest",
                expected: quote! {
                    impl From<()> for crate::protos::test::VoidRequest {
                        fn from(_: ()) -> Self {
                            Self {}
                        }
                    }
                },
            },
            TestCase {
                name: ".test.SingleMessageRequest",
                expected: quote! {
                    impl<T: Into<crate::protos::test::SingleMessage>> From<T> for crate::protos::test::SingleMessageRequest {
                        fn from(t: T) -> Self {
                            Self {
                                value: Some(t.into())
                            }
                        }
                    }
                },
            },
            TestCase {
                name: ".test.SinglePrimitiveRequest",
                expected: quote! {
                    impl<T: Into<::prost::alloc::string::String>> From<T> for crate::protos::test::SinglePrimitiveRequest {
                        fn from(t: T) -> Self {
                            Self {
                                value: t.into()
                            }
                        }
                    }
                },
            },
            TestCase {
                name: ".test.SingleOneOfMessageRequest",
                expected: quote! {
                    impl<T: Into<crate::protos::test::SingleMessage>> From<T> for crate::protos::test::SingleOneOfMessageRequest {
                        fn from(t: T) -> Self {
                            Self {
                                value: Some(crate::protos::test::single_one_of_message_request::Value::Inner(t.into()))
                            }
                        }
                    }
                },
            },
            TestCase {
                name: ".test.SingleOneOfPrimitiveRequest",
                expected: quote! {
                    impl<T: Into<::prost::alloc::string::String>> From<T> for crate::protos::test::SingleOneOfPrimitiveRequest {
                        fn from(t: T) -> Self {
                            Self {
                                value: Some(crate::protos::test::single_one_of_primitive_request::Value::Inner(t.into()))
                            }
                        }
                    }
                },
            },
            TestCase {
                name: ".test.ExternRequest",
                expected: quote! {
                    impl<T: Into<crate::CustomExternType>> From<T> for crate::protos::test::ExternRequest {
                        fn from(t: T) -> Self {
                            Self {
                                value: Some(t.into())
                            }
                        }
                    }
                },
            },
            TestCase {
                name: ".test.EmptyMessage",
                expected: quote! {
                    impl From<()> for crate::protos::test::EmptyMessage {
                        fn from(_: ()) -> Self {
                            Self {}
                        }
                    }
                },
            },
        ];

        for case in cases {
            let actual = generator
                .make_convenience_converter(message(&generator, case.name))
                .unwrap_or_else(|error| panic!("failed to generate {}: {error}", case.name))
                .unwrap_or_else(|| panic!("missing converter for {}", case.name));

            assert_tokens_eq(case.name, actual, case.expected);
        }

        let multi_request = generator
            .make_convenience_converter(message(&generator, ".test.MultiRequest"))
            .expect("multi-field converter check should not fail");
        assert!(
            multi_request.is_none(),
            "multi-field messages should not get convenience converters"
        );
    }
}
