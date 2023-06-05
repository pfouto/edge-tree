gradle shadowjar

echo "Copying to cluster"
rsync -azvhuP -r --delete --exclude 'venv' --exclude=".*" deploy/* cluster:/home/pfouto/edge/deploy
echo "Copying to grid"
rsync -azvhuP -r --delete --exclude 'venv' --exclude=".*" deploy/* nancy.g5k:/home/pfouto/edge/deploy
echo "Done"