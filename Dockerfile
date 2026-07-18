# Step 1: Use an official, lightweight Python base runtime environment
FROM python:3.11-slim

# Step 2: Establish the isolated operational workspace inside the container
WORKDIR /app

# Step 3: Install essential system security toolsets required for compiling cryptography wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Step 4: Move dependency blueprints first to optimize Docker's caching layers
COPY requirements.txt .

# Step 5: Upgrade pip and install all pinned production modules securely
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Step 6: Copy your entire framework project code and structural assets into the container image
COPY . .

# Step 7: Configure the container execution path to run your master orchestrator by default
ENTRYPOINT ["python", "master_orchestrator.py"]

# Step 8: Apply default execution parameters (can be overridden at runtime, e.g., --env prod)
CMD ["--env", "dev"]
