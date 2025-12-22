import subprocess
import re
from datetime import datetime
from typing import List, Dict

def get_git_commits(from_ref: str = None, to_ref: str = "HEAD") -> List[str]:
    """Get commit messages from git history."""
    cmd = ["git", "log", "--pretty=format:%s", to_ref]
    if from_ref:
        cmd.append(f"{from_ref}..{to_ref}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip().split('\n')
    except subprocess.CalledProcessError:
        return []

def get_latest_tag() -> str:
    """Get the latest git tag."""
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"], 
            capture_output=True, 
            text=True
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None

def parse_commits(commits: List[str]) -> Dict[str, List[str]]:
    """Parse commits into categories based on Conventional Commits."""
    categories = {
        "feat": "Added",
        "fix": "Fixed",
        "docs": "Documentation",
        "style": "Changed",
        "refactor": "Changed",
        "perf": "Changed",
        "test": "Fixed",
        "chore": "Changed"
    }
    
    grouped = {
        "Added": [],
        "Fixed": [],
        "Changed": [],
        "Documentation": [],
        "Other": []
    }
    
    for commit in commits:
        if not commit:
            continue
            
        # Regex for "type(scope): message" or "type: message"
        match = re.match(r"^(\w+)(?:\(.*\))?: (.*)$", commit)
        
        if match:
            type_ = match.group(1).lower()
            message = match.group(2)
            
            category = categories.get(type_, "Other")
            grouped[category].append(message)
        else:
            grouped["Other"].append(commit)
            
    return grouped

def generate_markdown(version: str, grouped_commits: Dict[str, List[str]]) -> str:
    """Generate Markdown for the release notes."""
    date = datetime.now().strftime("%Y-%m-%d")
    md = [f"## [{version}] - {date}", ""]
    
    # Order of sections
    sections = ["Added", "Changed", "Fixed", "Documentation", "Other"]
    
    for section in sections:
        commits = grouped_commits.get(section, [])
        if commits:
            md.append(f"### {section}")
            for commit in commits:
                md.append(f"- {commit}")
            md.append("")
            
    return "\n".join(md)

def generate_changelog(version: str) -> str:
    """Generate changelog for the given version."""
    latest_tag = get_latest_tag()
    commits = get_git_commits(from_ref=latest_tag)
    
    if not commits or (len(commits) == 1 and not commits[0]):
        return f"## [{version}] - {datetime.now().strftime('%Y-%m-%d')}\n\nNo changes detected."

    grouped = parse_commits(commits)
    return generate_markdown(version, grouped)
