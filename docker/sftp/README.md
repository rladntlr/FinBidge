# FinBridge local SFTP host keys

This directory contains fixed SSH host keys for the local `atmoz/sftp` Docker container.

They are for local demo use only. Keeping them here prevents `localhost:2222` from changing its host key every time the SFTP container is recreated.

If these files are replaced, refresh your local `known_hosts` entry:

macOS/Linux:

```bash
ssh-keygen -R "[localhost]:2222" -f "$HOME/.ssh/known_hosts"
ssh-keyscan -T 10 -p 2222 localhost >> "$HOME/.ssh/known_hosts"
```

Windows PowerShell:

```powershell
sftp -P 2222 finbridge@localhost
```

Type `yes` when OpenSSH asks whether to continue connecting, enter the local demo password `finbridge123`, then run `exit` at the `sftp>` prompt.

If your local Docker port mapping is changed, for example to `22022:22`, use that port instead:

```powershell
sftp -P 22022 finbridge@localhost
```
