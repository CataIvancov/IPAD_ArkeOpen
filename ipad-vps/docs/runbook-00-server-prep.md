# Runbook 00 — Server preparation (Ubuntu 24.04)

Base setup shared by both stacks. Run these **yourself over SSH**; paste output back if anything errors.

**Assumptions**

- Ubuntu 24.04 LTS, single VPS
- Public IP: `103.197.188.213` (referred to below as `VPS_IP`)
- You log in as sudo user `CataIvancov`, whose public key is already in `~/.ssh/authorized_keys`
- One 16 GB box hosting both Arches (dev) and ArkeOpen

## 0.1 Connect

From your own machine (not from here):

```bash
ssh CataIvancov@103.197.188.213
```

## 0.2 Update the system

```bash
sudo apt update && sudo apt -y upgrade
sudo apt -y install git curl ca-certificates gnupg build-essential ufw
```

## 0.3 Firewall (keep it tight for development)

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
# Arches dev server (only while developing; remove once behind nginx):
sudo ufw allow 8000/tcp
sudo ufw --force enable
sudo ufw status verbose
```

While there's no domain/HTTPS yet, prefer restricting to your own IP instead of the world:

```bash
# Example: only allow your home/office IP to reach 8000
# sudo ufw delete allow 8000/tcp
# sudo ufw allow from YOUR.HOME.IP.ADDR to any port 8000 proto tcp
```

## 0.4 Swap (safety margin for asset builds)

The Arches frontend build is memory-hungry. On a 16 GB box this is optional, but a swap file avoids OOM kills during `npm`/webpack builds:

```bash
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
free -h
```

## 0.5 nginx (shared reverse proxy)

```bash
sudo apt -y install nginx
sudo systemctl enable --now nginx
# Confirm it serves the default page:
curl -I http://127.0.0.1
```

## 0.6 Node.js (for Arches + ArkeOpen frontends)

Install Node 20 LTS from NodeSource:

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt -y install nodejs
node --version
npm --version
```

## 0.7 Directory layout

```bash
sudo mkdir -p /opt/ipad
sudo chown "$USER":"$USER" /opt/ipad
cd /opt/ipad
# Arches project        -> /opt/ipad/arches
# ArkeOpen source        -> /opt/ipad/arkeopen
```

Next: [`runbook-01-arches-native.md`](runbook-01-arches-native.md).
