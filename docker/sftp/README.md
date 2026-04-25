# FinBridge local SFTP host keys

This directory contains fixed SSH host keys for the local `atmoz/sftp` Docker container.

They are for local demo use only. Keeping them here prevents `localhost:2222` from changing its host key every time the SFTP container is recreated.

If these files are replaced, refresh your local `known_hosts` entry:

```bash
ssh-keygen -R "[localhost]:2222" -f "$HOME/.ssh/known_hosts"
ssh-keyscan -T 10 -p 2222 localhost >> "$HOME/.ssh/known_hosts"
```
