FROM python:3.13-alpine

# It's often good practice to create a non-root user
RUN adduser -D -h /home/user user

# Switch to non-root user
USER user

# Create a working directory
WORKDIR /home/user/app

# Copy in the build files first
COPY --chown=user:user pyproject.toml .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir .

# Copy application code
COPY --chown=user:user app /home/user/app

# Pre-compile Python bytecode (optional, but can speed up container startup)
RUN python -m compileall -f -b /home/user/app
RUN python -O -m compileall -f -b /home/user/app \
    && python -OO -m compileall -f -b /home/user/app

# Expose a port if needed (e.g., for metrics)
EXPOSE 8080

# Optionally set environment variables for Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# If you have an entrypoint script, copy and use it here
# Otherwise, you can start kopf directly, or run your main:
# ENTRYPOINT ["python", "-m", "kopf", "run", "/home/user/app/tuskr_controller.py"]

ENTRYPOINT ["python", "/home/user/app/main.py"]
