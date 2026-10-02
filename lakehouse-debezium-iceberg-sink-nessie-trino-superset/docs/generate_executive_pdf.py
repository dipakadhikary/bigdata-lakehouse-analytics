"""
Generate Smart Data Platform Executive Summary PDF.
Requires: pip install fpdf2
"""

from __future__ import annotations

from pathlib import Path

from fpdf import FPDF
from fpdf.enums import XPos, YPos

OUTPUT = Path(__file__).resolve().parent / "Smart-Data-Platform-Executive-Summary.pdf"

# Brand colors (RGB)
NAVY = (15, 76, 129)
DARK = (30, 41, 59)
BODY = (51, 65, 85)
MUTED = (100, 116, 139)
LIGHT_BG = (248, 250, 252)
ACCENT = (37, 99, 235)


class ExecutivePDF(FPDF):
    def header(self) -> None:
        self.set_fill_color(*NAVY)
        self.rect(0, 0, 210, 28, "F")
        self.set_xy(14, 8)
        self.set_font("Helvetica", "B", 18)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, "Smart Data Platform", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_x(14)
        self.set_font("Helvetica", "", 10)
        self.set_text_color(200, 220, 240)
        self.cell(0, 6, "Data Lakehouse - Executive Summary", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_xy(140, 8)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(200, 220, 240)
        self.multi_cell(
            56,
            4,
            "Version 1.0\nJune 2026\nClient Presentation",
            align="R",
        )
        self.set_y(32)

    def footer(self) -> None:
        self.set_y(-12)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(*MUTED)
        self.cell(
            0,
            5,
            "Smart Data Platform | Data Lakehouse Architecture | Confidential",
            align="C",
        )

    def section_title(self, title: str) -> None:
        self.ln(3)
        self.set_fill_color(*NAVY)
        self.rect(14, self.get_y(), 3, 7, "F")
        self.set_x(19)
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*NAVY)
        self.cell(0, 7, title.upper(), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)

    def body_text(self, text: str) -> None:
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*BODY)
        self.multi_cell(0, 4.5, text)
        self.ln(2)

    def highlight_box(self, text: str) -> None:
        y = self.get_y()
        self.set_fill_color(240, 247, 255)
        self.set_draw_color(207, 227, 245)
        self.rect(14, y, 182, 22, "DF")
        self.set_xy(17, y + 3)
        self.set_font("Helvetica", "", 9.5)
        self.set_text_color(*DARK)
        self.multi_cell(176, 4.5, text)
        self.set_y(y + 24)

    def two_column_outcomes(self) -> None:
        outcomes = [
            (
                "Near-Real-Time Analytics",
                "Operational data queryable within minutes, not overnight batches.",
            ),
            (
                "Single Source of Truth",
                "Versioned Iceberg tables for consistent, auditable reporting.",
            ),
            (
                "Lower Integration Cost",
                "Standard SQL and BI tools reduce specialized skill dependency.",
            ),
            (
                "Future-Ready Foundation",
                "New domains onboard without architectural redesign.",
            ),
        ]
        col_w = 89
        start_y = self.get_y()
        for i, (title, desc) in enumerate(outcomes):
            col = i % 2
            row = i // 2
            x = 14 + col * (col_w + 4)
            y = start_y + row * 18
            self.set_xy(x, y)
            self.set_fill_color(255, 255, 255)
            self.set_draw_color(219, 234, 254)
            self.rect(x, y, col_w, 16, "DF")
            self.set_fill_color(*ACCENT)
            self.rect(x, y, 2, 16, "F")
            self.set_xy(x + 4, y + 2)
            self.set_font("Helvetica", "B", 8)
            self.set_text_color(30, 64, 175)
            self.cell(col_w - 6, 4, title, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            self.set_x(x + 4)
            self.set_font("Helvetica", "", 7.5)
            self.set_text_color(*BODY)
            self.multi_cell(col_w - 6, 3.5, desc)
        self.set_y(start_y + 38)

    def flow_diagram(self) -> None:
        steps = [
            ("PostgreSQL", "Source"),
            ("Debezium", "CDC"),
            ("Kafka", "Stream"),
            ("Iceberg", "Lake"),
            ("Trino", "SQL"),
            ("Superset", "BI"),
        ]
        box_w = 26
        gap = 3.5
        start_x = 14
        y = self.get_y()
        x = start_x
        for i, (name, sub) in enumerate(steps):
            self.set_fill_color(*NAVY)
            self.rect(x, y, box_w, 12, "F")
            self.set_xy(x, y + 1.5)
            self.set_font("Helvetica", "B", 7)
            self.set_text_color(255, 255, 255)
            self.cell(box_w, 4, name, align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            self.set_x(x)
            self.set_font("Helvetica", "", 6)
            self.set_text_color(200, 220, 240)
            self.cell(box_w, 3, sub, align="C")
            x += box_w
            if i < len(steps) - 1:
                self.set_xy(x, y + 4)
                self.set_font("Helvetica", "B", 9)
                self.set_text_color(*MUTED)
                self.cell(gap, 5, ">", align="C")
                x += gap
        self.set_y(y + 16)

    def table(self, headers: list[str], rows: list[list[str]], col_widths: list[int]) -> None:
        self.set_font("Helvetica", "B", 7.5)
        self.set_fill_color(*NAVY)
        self.set_text_color(255, 255, 255)
        for h, w in zip(headers, col_widths):
            self.cell(w, 6, h, border=1, fill=True)
        self.ln()
        self.set_font("Helvetica", "", 7.5)
        self.set_text_color(*BODY)
        fill = False
        for row in rows:
            if fill:
                self.set_fill_color(*LIGHT_BG)
            else:
                self.set_fill_color(255, 255, 255)
            max_h = 6
            for cell, w in zip(row, col_widths):
                self.cell(w, max_h, cell, border=1, fill=True)
            self.ln()
            fill = not fill
        self.ln(2)


def build_pdf() -> Path:
    pdf = ExecutivePDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=14)
    pdf.add_page()

    pdf.highlight_box(
        "The Smart Data Platform is a modern data lakehouse that unifies operational and "
        "analytical workloads. It captures database changes in near real time, stores them as "
        "governed Iceberg tables on object storage, and delivers insights through standard SQL "
        "and self-service dashboards - built entirely on open-source, enterprise-grade technology."
    )

    pdf.section_title("Business Value")
    pdf.two_column_outcomes()

    pdf.section_title("Solution Overview")
    pdf.body_text(
        "The platform replaces traditional batch ETL with Change Data Capture (CDC). Every insert, "
        "update, and delete in PostgreSQL is streamed through Kafka, materialized as Apache Iceberg "
        "tables, and made available for analytics - with full schema evolution, time travel, and ACID "
        "guarantees."
    )
    pdf.flow_diagram()

    pdf.section_title("Architecture Stack")
    pdf.table(
        ["Layer", "Technology", "Role"],
        [
            ["Source", "PostgreSQL 16", "Operational database and CDC origin"],
            ["Streaming", "Apache Kafka 4.1", "Durable event backbone"],
            ["Ingestion", "Debezium + Iceberg Connect", "CDC capture and lakehouse writes"],
            ["Storage", "MinIO (S3-compatible)", "Columnar data files (Parquet)"],
            ["Catalog", "Project Nessie", "Versioned table metadata"],
            ["Compute", "Trino 480", "Distributed SQL engine"],
            ["BI", "Apache Superset", "Self-service dashboards"],
        ],
        [28, 42, 112],
    )

    pdf.section_title("Medallion Data Layers")
    pdf.table(
        ["Layer", "Purpose", "Examples"],
        [
            ["Bronze", "Raw CDC landing; full change history", "merchants, transactions"],
            ["Silver", "Cleansed, deduplicated business entities", "payments, merchant_risk"],
            ["Gold", "KPIs and executive reporting", "daily_gmv, success_rate"],
        ],
        [22, 78, 82],
    )

    pdf.section_title("Production Evolution Path")
    pdf.table(
        ["Component", "Current (POC)", "Production Target"],
        [
            ["Kafka", "Single node", "Multi-broker cluster (RF >= 3)"],
            ["Object Storage", "MinIO", "AWS S3 / Azure / GCS"],
            ["Catalog", "Nessie (in-memory)", "Nessie with persistent backend"],
            ["Query Engine", "Single Trino node", "Trino cluster with workers"],
            ["Security", "Development defaults", "TLS, SSO, IAM, secrets vault"],
            ["Orchestration", "Docker Compose", "Kubernetes with auto-scaling"],
        ],
        [38, 52, 92],
    )

    pdf.section_title("Key Differentiators")
    pdf.body_text(
        "Open & Portable: 100% open-source; no vendor lock-in; production-ready on any cloud. "
        "Governed & Auditable: Iceberg snapshots, Nessie version branches, immutable Kafka event log. "
        "Scalable by Design: independent storage/compute scaling; containerized; clear POC-to-production path."
    )

    pdf.output(str(OUTPUT))
    return OUTPUT


if __name__ == "__main__":
    path = build_pdf()
    print(f"PDF generated: {path}")
