from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime


@dataclass
class Review:
    state: str


@dataclass
class CheckRun:
    name: str
    conclusion: Optional[str]
    completed_at: Optional[str]


@dataclass
class PullRequest:
    number: int
    title: str
    user_login: str
    user_id: int
    repository: str
    created_at: str
    merged_at: Optional[str]
    closed_at: Optional[str]
    base_branch: str
    head_branch: str
    reviews: List[Dict] = field(default_factory=list)
    approved_reviews: int = 0
    check_runs: List[Dict] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            'number': self.number,
            'title': self.title,
            'user_login': self.user_login,
            'user_id': self.user_id,
            'repository': self.repository,
            'created_at': self.created_at,
            'merged_at': self.merged_at,
            'closed_at': self.closed_at,
            'base_branch': self.base_branch,
            'head_branch': self.head_branch,
            'reviews': self.reviews,
            'approved_reviews': self.approved_reviews,
            'check_runs': self.check_runs,
        }


@dataclass
class ComplianceSummary:
    total_prs: int
    compliant_prs: int
    compliance_rate: float

    def to_dict(self) -> Dict:
        return {
            'total_prs': self.total_prs,
            'compliant_prs': self.compliant_prs,
            'compliance_rate': self.compliance_rate,
        }
