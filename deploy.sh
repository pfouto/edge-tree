
echo "Copying to cluster"
rsync -azvhuP -r --delete --exclude 'venv' --exclude=".*" deploy/* cluster:/home/pfouto/edge/deploy
#echo "Copying to grid"
#rsync -azvhuP -r --delete --exclude 'venv' --exclude=".*" . nancy.g5k:/home/pfouto/edge/exp_management
echo "Done"