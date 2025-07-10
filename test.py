import random
import os

# Global set to track uploaded files
uploaded_files = set()


def generate_random_command():
    """Generate a random command with parameters."""
    command = random.randint(2, 4)  # Commands: upload, download, update, sleep
    if command == 2:  # Upload
        filepath = generate_test_file()
        uploaded_files.add(os.path.basename(filepath))  # Track uploaded file
        return f"2\n{filepath}\n"
    elif command == 3:  # Download
        if not uploaded_files:
            # No files uploaded, force an upload
            return generate_random_command_with_upload()
        filename = random.choice(list(uploaded_files))
        file_size = os.path.getsize(filename)
        startpos = random.randint(0, file_size - 1)
        offset = random.randint(1, min(512, file_size - startpos))
        return f"3\n{filename}\n{startpos}\n{offset}\n"
    elif command == 4:  # Update
        if not uploaded_files:
            # No files uploaded, force an upload
            return generate_random_command_with_upload()
        filename = random.choice(list(uploaded_files))
        startpos = random.randint(0, 512)  # Within reasonable range
        datatowrite = generate_random_data()
        return f"4\n{filename}\n{startpos}\n{datatowrite}\n"
    elif command == 5:  # Sleep
        time_to_sleep = random.uniform(0, max_interval)
        return f"5\n{time_to_sleep}\n"


def generate_random_command_with_upload():
    """Force an upload command to ensure files exist."""
    filepath = generate_test_file()
    uploaded_files.add(os.path.basename(filepath))  # Track uploaded file
    return f"2\n{filepath}\n"


def generate_test_file():
    """Generate a random test file."""
    filepath = f"test_file_{random.randint(1000, 9999)}.txt"
    with open(filepath, "w") as file:
        file.write("".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=32)))
    return os.path.abspath(filepath)


def generate_random_data():
    """Generate random data to write."""
    return "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=random.randint(32, 32)))


def simulate_requests(num_requests):
    """Simulate requests and store them in a file."""
    with open("input.txt", "w") as file:
        for i in range(2*num_requests):
            command = generate_random_command()
            if i % 2 == 1:
                time_to_sleep = random.uniform(0, max_interval)
                command = f"5\n{time_to_sleep}\n"
                # command = generate_random_command_with_upload
            if i == 2*num_requests - 1:  # Ensure the last command is exit
                command = "6\n"
            file.write(command)
            print(f"Stored command:\n{command.strip()}")


if __name__ == "__main__":
    num_requests = int(input("Enter the number of requests to simulate: "))
    max_interval = float(
        input("Enter the maximum interval (in seconds) between requests: "))

    simulate_requests(num_requests)
