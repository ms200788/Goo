FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY bot.py /app/bot.py

# Set environment
ENV PYTHONIOENCODING=UTF-8
ENV LANG=C.UTF-8
ENV PORT=10000

# Run bot
CMD ["python", "bot.py"]
