import os
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib.ticker import FixedLocator
from brokenaxes import brokenaxes
import matplotlib.font_manager as fm
import matplotlib as mpl

# ---- FONT SETUP ----
FONT_FAMILY = "Times New Roman"

# Check if the font is available
available_fonts = sorted(set(f.name for f in fm.fontManager.ttflist))
if FONT_FAMILY in available_fonts:
    print(f"✅ '{FONT_FAMILY}' is available.")
else:
    print(f"⚠️ '{FONT_FAMILY}' not found. Falling back to default.")

# Set the font globally
mpl.rcParams['font.family'] = FONT_FAMILY  # Force font globally
mpl.rcParams.update({'mathtext.default': 'regular'})  # Ensure font applies to all text

# Define a common font dictionary for customization
FONT_DICT = {'family': FONT_FAMILY, 'size': 14}  # Bigger font size for tick labels
LABEL_FONT_DICT = {'family': FONT_FAMILY, 'size': 16}  # Bigger font size for axis labels


# ---- PLOTTING FUNCTION ----
def helper_plot(x_values, accuracy_values):
    """
    Plots accuracy values vs. x_values with a broken y-axis (80–90, 98–100),
    and x-axis ticks are forced to [1, 3, 7, 64].
    Y-axis ticks are set to specific values.
    """

    # Print the first 10 accuracy values for debugging
    for i in range(min(10, len(accuracy_values))):  # Avoid index errors
        print(f'{i + 1} : {accuracy_values[i]}')

    # Set Seaborn style
    sns.set_style("whitegrid")

    # Create figure with broken y-axis
    fig = plt.figure(figsize=(12, 4))
    bax = brokenaxes(ylims=((78, 80), (97, 100.2)), hspace=0.1)

    # Plot accuracy
    bax.plot(x_values, accuracy_values, marker='o', linestyle='-', color='blue', label='Accuracy (%)')

    # Access the top and bottom subplot axes
    top_ax, bot_ax = bax.axs

    # ---- Y-axis customization ----
    bot_ax.set_yticks([78, 80, 79.13])
    bot_ax.set_yticklabels(['78', '80', '79.13'], fontdict=FONT_DICT)

    top_ax.set_yticks([100, 98.06, 97, 99.19, 99.59])
    top_ax.set_yticklabels(['100', '98.06', '97', '99.19', '99.59'], fontdict=FONT_DICT)

    # ---- X-axis customization ----
    for ax in (top_ax, bot_ax):
        ax.set_xlim(0, 64)
        ax.xaxis.set_major_locator(FixedLocator([1, 3, 7, 64]))
        ax.set_xticklabels(['1', '3', '7', '64'], fontdict=FONT_DICT)

        # ---- Increase Tick Size ----
        ax.tick_params(axis='both', which='major', labelsize=14, length=8, width=2)  # Bigger ticks
        ax.tick_params(axis='both', which='minor', length=4, width=1.5)  # Minor ticks smaller

    # ---- Set Axis Labels ----
    bax.set_xlabel('Number of Vectors', fontdict=LABEL_FONT_DICT)
    bax.set_ylabel('Accuracy (%)', fontdict=LABEL_FONT_DICT, labelpad=45)

    # ---- Ensure Full Black Frame (Left, Right, Top, Bottom) ----
    for ax in (top_ax, bot_ax):
        for spine_name, spine in ax.spines.items():
            spine.set_visible(True)  # Ensure spine is visible
            spine.set_edgecolor("black")  # Make frame black
            spine.set_linewidth(2)  # Increase frame thickness

    # ---- Move Legend to Bottom Right ----
    handles, labels = bot_ax.get_legend_handles_labels()
    bax.legend(handles, labels, fontsize=14, prop=FONT_DICT, loc='lower right', frameon=True, edgecolor="black")

    # ---- Save the Plot ----
    result_dir = "../paper_result/sampling_benchmark/"
    os.makedirs(result_dir, exist_ok=True)

    # Save in multiple formats to preserve font settings
    plt.savefig(os.path.join(result_dir, "sampling_benchmark_accuracy.svg"), format='svg', bbox_inches='tight')
    plt.savefig(os.path.join(result_dir, "sampling_benchmark_accuracy.pdf"), format='pdf', bbox_inches='tight')

    print(f"✅ Plot saved to: {result_dir}")


# ---- EXAMPLE USAGE ----
if __name__ == "__main__":
    # Example data (replace with actual values)
    x_values = [1, 3, 7, 16, 21, 32, 37, 64]
    accuracy_values = [79.13, 98.06, 99.19, 99.30, 99.40, 99.55, 99.60, 99.40]

    helper_plot(x_values, accuracy_values)
