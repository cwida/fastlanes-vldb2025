import csv
import re
from collections import Counter

def extract_first_bracketed(expression):
    """
    If the expression is something like:
      "{[EXP_DICT_I32_FFOR_U16][...]}",
    extract just:
      "EXP_DICT_I32_FFOR_U16".

    Returns the original string if format doesn't match.
    """
    pattern = r"^\{\[([^]]+)\]\[.*\]\}$"
    match = re.match(pattern, expression)
    if match:
        return match.group(1)
    else:
        return expression

def normalize_expression(expr):
    """
    1) Remove leading "EXP_" if present.
    2) Replace I16/I32/I64 -> INTEGER.
    3) Replace _U08/_U16/_U32/_U64 -> _UX.
    """
    # 1) Remove leading "EXP_" if it exists (e.g., "EXP_FSST..." -> "FSST...")
    expr = re.sub(r"^EXP_", "", expr)

    # 2) Convert I16/I32/I64 -> INTEGER
    expr = re.sub(r"I(16|32|64)", "INTEGER", expr)

    # 3) Convert _U08/_U16/_U32/_U64 -> _UX
    expr = re.sub(r"_U(08|16|32|64)", "_UX", expr)

    return expr

def categorize_expression(expr):
    """
    Returns (category_id, category_name) to determine sorting/grouping.

    Order of preference:
      1. Contains "STR"     -> (1, "STR Expressions")
      2. Contains "INTEGER" -> (2, "INTEGER Expressions")
      3. Contains "DBL"     -> (3, "DBL Expressions")
      4. Otherwise          -> (4, "Others")
    """
    expr_upper = expr.upper()

    if "STR" in expr_upper:
        return (1, "STR Expressions")
    elif "INTEGER" in expr_upper:
        return (2, "INTEGER Expressions")
    elif "DBL" in expr_upper:
        return (3, "DBL Expressions")
    else:
        return (4, "Others")

def create_latex_table_sorted(counts, total, caption_label):
    """
    Creates a single LaTeX table sorted into categories:
      STR → INTEGER → DBL → Others,
    each sorted by descending popularity.
    """
    # Build a list of (expr, count, popularity, cat_id, cat_name)
    rows = []
    for expr, count in counts.items():
        popularity = (count / total * 100) if total > 0 else 0
        cat_id, cat_name = categorize_expression(expr)
        rows.append((expr, count, popularity, cat_id, cat_name))

    # Sort by (category_id ascending, then popularity descending)
    rows.sort(key=lambda r: (r[3], -r[2]))

    # Build the LaTeX table
    lines = []
    lines.append(r"\begin{table}[ht]")
    lines.append(r"\centering")
    lines.append(r"\begin{tabular}{|l|r|r|}")
    lines.append(r"\hline")
    lines.append(r"Expression & Count & Popularity (\%) \\ \hline")

    current_cat_name = None
    for (expr, count, pop, cat_id, cat_name) in rows:
        # If we reach a new category, insert a sub-header
        if cat_name != current_cat_name:
            lines.append(r"\multicolumn{3}{|l|}{\textbf{%s}} \\" % cat_name)
            lines.append(r"\hline")
            current_cat_name = cat_name

        # Replace underscores with \_ for LaTeX
        display_expr = expr.replace("_", r"\_")

        lines.append(f"{display_expr} & {count} & {pop:.2f}\\% \\\\ \\hline")

    lines.append(r"\end{tabular}")
    lines.append(f"\\caption{{{caption_label}}}")
    lines.append(r"\end{table}")

    return "\n".join(lines)

def count_expressions_in_csv(csv_path):
    """
    Reads the CSV file, extracts & normalizes expressions,
    and counts the occurrences.
    """
    expression_counts = Counter()

    with open(csv_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_expr = row['expression']

            # 1. Extract bracketed portion
            bracketed_expr = extract_first_bracketed(raw_expr)

            # 2. Normalize (remove "EXP_", do I->INTEGER replacements, etc.)
            normalized_expr = normalize_expression(bracketed_expr)

            # Count
            expression_counts[normalized_expr] += 1

    return expression_counts

if __name__ == "__main__":
    # Update with your actual CSV path
    csv_file = "../fastlanes/benchmark/result/compression_ratio/public_bi/fastlanes_detailed.csv"

    counts = count_expressions_in_csv(csv_file)
    total = sum(counts.values())

    latex_table = create_latex_table_sorted(
        counts,
        total,
        caption_label="Expressions Sorted by Category & Popularity"
    )

    print(latex_table)
