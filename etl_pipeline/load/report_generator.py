
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger("etl.report")

class ReportGenerator:
    def __init__(self, cfg):
        self.cfg = cfg
        self.out_dir = Path(cfg["output"]["out_dir"])
        self.env = Environment(loader=FileSystemLoader(searchpath="."), autoescape=select_autoescape(["html"]))

    def generate_html_report(self, sections: dict):
        # sections: dict[str, DataFrame]
        template_str = """
        <html>
        <head>
          <meta charset="utf-8"/>
          <title>ETL Summary Report</title>
          <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            table { border-collapse: collapse; margin-bottom: 20px; }
            th, td { border: 1px solid #ccc; padding: 6px 8px; }
            h1, h2 { color: #222; }
          </style>
        </head>
        <body>
          <h1>ETL Summary Report</h1>
          {% for k,v in sections.items() %}
            <h2>{{ k }}</h2>
            {% if v is none or v.empty %}
              <p>No data</p>
            {% else %}
              {{ v.head(10).to_html(classes="table", index=False) | safe }}
            {% endif %}
          {% endfor %}
        </body>
        </html>
        """
        template = self.env.from_string(template_str)
        rendered = template.render(sections=sections)
        out_path = self.out_dir / "report.html"
        out_path.write_text(rendered, encoding="utf-8")
        logger.info(f"Wrote HTML report to {out_path}")

    # If PDF support is desired, add method using reportlab or wkhtmltopdf (not included by default).
