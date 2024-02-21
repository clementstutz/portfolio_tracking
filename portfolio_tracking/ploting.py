import matplotlib.pyplot as plt


def plot_single_chart(dates_and_prices, value, figure_id, title, xlabel, ylabel, style='', xscale='linear', yscale='linear'):
    plt.figure(figure_id)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.xscale(xscale)
    plt.ylabel(ylabel)
    plt.yscale(yscale)
    ymin = min(0, min(dates_and_prices[value][:])*1.05)
    ymax = max(0, max(dates_and_prices[value][:])*1.05)
    plt.ylim(ymin, ymax)
    plt.grid(True)
    plt.plot(dates_and_prices['dates'][:], dates_and_prices[value][:], style, label=value)
    plt.legend()
    plt.show(block=False)


def plot_multi_charts(dates_and_prices, values, figure_id, title, xlabel, ylabel, xscale='linear', yscale='linear'):
    plt.figure(figure_id)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.xscale(xscale)
    plt.ylabel(ylabel)
    plt.yscale(yscale)
    ymin = 0
    ymax = 0
    for value in values:
        ymin = min(ymin, min(dates_and_prices[value][:])*1.05)
        ymax = max(ymax, max(dates_and_prices[value][:])*1.05)
    plt.ylim(ymin, ymax)
    plt.grid(True)
    for value in values:
        plt.plot(dates_and_prices['dates'][:], dates_and_prices[value][:], label=value)
    plt.legend()
    plt.show(block=False)

