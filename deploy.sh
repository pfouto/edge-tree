gradle shadowjar

echo "Copying to cluster"
rsync -azvhuP -r  --delete --exclude 'venv' --exclude=".*" deploy/* cluster:/home/pfouto/edge/server
#echo "Copying to grid"
#rsync -azvhuP -r --delete --exclude 'venv' --exclude=".*" deploy/* nancy.g5k:/home/pfouto/edge/server
echo "Done"