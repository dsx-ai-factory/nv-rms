# Documentation diagrams

Editable draw.io sources and their rendered images for the docs site. The
Markdown under `docs/` references the rendered `.svg` files.

| Diagram | Source | Rendered |
| --- | --- | --- |
| Northbound / southbound connections | `northbound-southbound.drawio` | `northbound-southbound.svg` / `.png` |
| Internal component layers | `internal-components.drawio` | `internal-components.svg` / `.png` |
| Kubernetes deployment model | `kubernetes-deployment.drawio` | `kubernetes-deployment.svg` / `.png` |

## Editing

1. Open the `.drawio` file in [draw.io](https://app.diagrams.net) (desktop or web).
2. Make your changes.
3. Export **both** an SVG and a PNG next to the source
   (`File → Export as → SVG` / `PNG`), keeping the same base filename.

If you have `rsvg-convert` installed, you can regenerate the PNG from an exported
SVG instead of exporting it separately:

```bash
rsvg-convert -z 2 northbound-southbound.svg -o northbound-southbound.png
```

Keep the source and rendered files in sync in the same commit.
