# scripts/utility_run_contradiction_gate.py
# Purpose: An interactive script to formalize the "Contradiction Gate" protocol.
# It guides the user through a checklist combining high-level strategic goals
# (the "User's Protocol") with tactical implementation checks (the "AI's Protocol").
# It generates a timestamped log for accountability and declares a final PASS/FAIL.

import sys
from pathlib import Path
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm

# --- Configuration ---
# Attempt to get the project root to save the log in the right place
try:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from compass_brca.pipeline_config import PROJECT_ROOT
    LOGS_DIR = PROJECT_ROOT / "logs"
except ImportError:
    # Fallback if run before project is fully set up
    LOGS_DIR = Path("logs")

LOG_FILE = LOGS_DIR / "contradiction_gate_log.md"

def ask_question(prompt_text: str, color: str = "cyan") -> str:
    """Helper to ask a text question with consistent styling."""
    return Prompt.ask(f"[bold {color}]{prompt_text}[/bold {color}]")

def ask_yes_no(prompt_text: str) -> bool:
    """Helper to ask a confirmation (Yes/No) question."""
    return Confirm.ask(f"[bold yellow]{prompt_text}[/bold yellow]", default=True)

def log_entry(header: str, content: str):
    """Appends a formatted entry to the markdown log file."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"### {header}\n")
        # Indent content for nice markdown blockquote formatting
        formatted_content = "> " + content.replace("\n", "\n> ")
        f.write(f"{formatted_content}\n\n")

def main():
    """Runs the interactive contradiction gate checklist."""
    console = Console()
    LOGS_DIR.mkdir(exist_ok=True)

    # --- Initialize Log ---
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n---\n## Contradiction Gate Run: {timestamp}\n\n")

    console.rule("[bold magenta]Contradiction Gate Protocol[/bold magenta]")
    
    # --- PHASE 1: Strategic Alignment (Your Protocol) ---
    console.print(Panel("Phase 1: Strategic Alignment (The 'Why')", style="bold blue"))
    q1_goal = ask_question("What is the primary scientific goal of the project?")
    log_entry("Primary Scientific Goal", q1_goal)

    q2_task = ask_question("What is the specific task/instruction for the NEXT script/action?")
    log_entry("Specific Task/Instruction", q2_task)

    q3_align = ask_yes_no("Does this task directly support the primary scientific goal?")
    log_entry("Task Aligns with Goal?", "Yes" if q3_align else "No")
    
    # --- PHASE 2: Tactical Plan (The 'BEFORE' Gate) ---
    console.print(Panel("Phase 2: Tactical Plan (The 'How')", style="bold blue"))
    q4_solution = ask_question("What is the proposed technical solution or plan?")
    log_entry("Proposed Technical Solution", q4_solution)
    
    q5_contradicts = ask_yes_no("Does this solution contradict any KNOWN project constraints (e.g., memory limits, existing architecture, data formats)?")
    log_entry("Plan Contradicts Constraints?", "Yes" if q5_contradicts else "No")

    # --- PHASE 3: Code Validation (The 'AFTER' Gate) ---
    console.print(Panel("Phase 3: Code Validation (The Final Check)", style="bold blue"))
    q6_logic = ask_yes_no("Does the generated code LOGICALLY implement the proposed solution?")
    log_entry("Code Implements Logic?", "Yes" if q6_logic else "No")

    q7_syntax = ask_yes_no("Has the code been checked for SYNTAX and SEMANTIC errors (e.g., valid API calls)?")
    log_entry("Code Syntax/Semantics Valid?", "Yes" if q7_syntax else "No")
    
    q8_complete = ask_yes_no("Does the code fulfill ALL aspects of the specific task/instruction from Phase 1?")
    log_entry("Code Fulfills Full Instruction?", "Yes" if q8_complete else "No")
    
    # --- FINAL VERDICT ---
    console.rule("[bold magenta]Final Verdict[/bold magenta]")
    
    # A failure on any key question fails the gate.
    # q5 is inverted: if it *does* contradict, it's a failure.
    gate_passed = q3_align and not q5_contradicts and q6_logic and q7_syntax and q8_complete

    if gate_passed:
        final_panel = Panel("[bold green]GATE PASSED[/bold green]\nThe proposed action is consistent with project goals and technical constraints.", title="[green]Result[/green]")
        log_entry("Final Verdict", "GATE PASSED")
    else:
        failures = []
        if not q3_align: failures.append("Task is not aligned with the scientific goal.")
        if q5_contradicts: failures.append("Proposed solution contradicts known constraints.")
        if not q6_logic: failures.append("Generated code does not logically implement the solution.")
        if not q7_syntax: failures.append("Code has not been validated for technical errors.")
        if not q8_complete: failures.append("Code is an incomplete solution for the task.")
        
        failure_text = "\n".join(f"- {f}" for f in failures)
        final_panel = Panel(f"[bold red]GATE FAILED[/bold red]\nReason(s):\n{failure_text}", title="[red]Result[/red]")
        log_entry("Final Verdict", f"GATE FAILED\n\nReasons:\n{failure_text}")

    console.print(final_panel)
    console.print(f"A detailed record of this check has been saved to: [cyan]{LOG_FILE}[/cyan]")

if __name__ == "__main__":
    main()