"""
evaluation.py
-------------
Compute and present model quality metrics on a held-out test set:
accuracy, macro precision / recall / F1, and a confusion matrix.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    precision_recall_fscore_support,
)


@dataclass(frozen=True)
class EvaluationReport:
    accuracy: float
    precision: float          # macro-averaged
    recall: float             # macro-averaged
    f1: float                 # macro-averaged
    labels: list[str]
    confusion: list[list[int]]
    support: int
    per_class: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "accuracy": round(self.accuracy, 4),
            "precision_macro": round(self.precision, 4),
            "recall_macro": round(self.recall, 4),
            "f1_macro": round(self.f1, 4),
            "support": self.support,
            "labels": self.labels,
            "confusion": self.confusion,
            "per_class": self.per_class,
        }

    def format_text(self) -> str:
        lines = [
            f"accuracy : {self.accuracy:.4f}",
            f"precision: {self.precision:.4f} (macro)",
            f"recall   : {self.recall:.4f} (macro)",
            f"f1       : {self.f1:.4f} (macro)",
            f"support  : {self.support}",
            "confusion matrix (rows = true, cols = pred):",
            "          " + "  ".join(f"{l:>9}" for l in self.labels),
        ]
        for label, row in zip(self.labels, self.confusion):
            lines.append(f"{label:>9} " + "  ".join(f"{v:>9d}" for v in row))
        return "\n".join(lines)


def evaluate(
    y_true: Sequence[str],
    y_pred: Sequence[str],
    labels: Sequence[str],
) -> EvaluationReport:
    labels = list(labels)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, labels=labels, average="macro", zero_division=0
    )
    p_c, r_c, f_c, s_c = precision_recall_fscore_support(
        y_true, y_pred, labels=labels, average=None, zero_division=0
    )
    per_class = {
        label: {
            "precision": round(float(p), 4),
            "recall": round(float(r), 4),
            "f1": round(float(f), 4),
            "support": int(s),
        }
        for label, p, r, f, s in zip(labels, p_c, r_c, f_c, s_c)
    }
    matrix = confusion_matrix(y_true, y_pred, labels=labels).tolist()
    return EvaluationReport(
        accuracy=float(accuracy_score(y_true, y_pred)),
        precision=float(precision),
        recall=float(recall),
        f1=float(f1),
        labels=labels,
        confusion=matrix,
        support=len(y_true),
        per_class=per_class,
    )