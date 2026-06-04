# Linux/macOS
curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs -o $HOME/minio-binaries/mc
chmod +x $HOME/minio-binaries/mc
export PATH=$PATH:$HOME/minio-binaries

# Crée un alias vers ton MinIO local
mc alias set local http://localhost:9000 minioadmin minioadmin123

# Vérifie
mc ls local