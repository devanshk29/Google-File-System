import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np

def parse_file(filename):
    x_values = []
    y_values = []
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip().strip("{}")
            x, y = map(float, line.split(','))
            x_values.append(x)
            y_values.append(y)
    return x_values, y_values

def animate(i, x_values, y_values, times, x_line, y_line):
    x_line.set_data(times[:i+1], x_values[:i+1])  # Update x vs t
    y_line.set_data(times[:i+1], y_values[:i+1])  # Update y vs t
    return x_line, y_line

def create_video(filename, output_video):
    x_values, y_values = parse_file(filename)
    times = np.arange(len(x_values))  # Time index, assuming 1 second per entry

    # Create the figure and axes
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 10))
    
    # Plot settings for x vs t
    ax1.set_xlim(0, len(times))
    ax1.set_ylim(min(x_values) - 1, max(x_values) + 1)
    ax1.set_xlabel("Time (t)")
    ax1.set_ylabel("X")
    ax1.set_title("Load vs Time")
    x_line, = ax1.plot([], [], color='blue', label="X vs t")
    ax1.legend()
    ax1.grid()

    # Plot settings for y vs t
    ax2.set_xlim(0, len(times))
    ax2.set_ylim(min(y_values) - 1, max(y_values) + 1)
    ax2.set_xlabel("Time (t)")
    ax2.set_ylabel("Y")
    ax2.set_title("No of Replicas vs Time")
    y_line, = ax2.plot([], [], color='red', label="Y vs t")
    ax2.legend()
    ax2.grid()

    # Animation
    ani = animation.FuncAnimation(
        fig, animate, frames=len(times), fargs=(x_values, y_values, times, x_line, y_line),
        interval=1000, blit=True  # 1 frame per second
    )

    # Save video
    ani.save(output_video, writer="ffmpeg", fps=1)
    print(f"Video saved as {output_video}")

# Example usage
input_file = 'final2.txt'  # Replace with your file path
output_file = 'graph_building.mp4'
create_video(input_file, output_file)