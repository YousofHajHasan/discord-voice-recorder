FROM ubuntu:22.04

# Prevent timezone prompts from freezing the build, I don't know what is this.
ENV DEBIAN_FRONTEND=noninteractive 

# Install build tools, FFmpeg, and required libraries
RUN apt-get update && \
    apt-get install -y g++ wget ffmpeg libopus0 libssl-dev zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Download and install the stable D++ library
RUN wget -O dpp-stable.deb https://github.com/brainboxdotcc/DPP/releases/download/v10.1.4/libdpp-10.1.4-linux-x64.deb && \
    dpkg -i dpp-stable.deb && \
    rm dpp-stable.deb

# Copy your source code
COPY bot.cpp .

# Compile the bot inside the container
RUN g++ -std=c++17 -o bot bot.cpp -ldpp

# Run the compiled bot
CMD ["./bot"]