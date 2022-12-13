# 停止和删除旧容器
OLD_CONTAINER_ID=`docker container ls -aq --filter name=quant-toolbox`
if [ -n $OLD_CONTAINER_ID ];then
    docker container stop $OLD_CONTAINER_ID
    docker container rm $OLD_CONTAINER_ID
fi
echo "stop and rm old container, done!"


# 删除旧镜像
OLD_IMAGE_ID=`docker image ls quant-toolbox -q`
if [ -n $OLD_IMAGE_ID ]; then
    docker image rm $OLD_IMAGE_ID
fi

echo "rm old image, done!"

# 加载新镜像
docker image load -i quant-toolbox.tar.gz
echo "load new container, done!"

# 启用新容器
IMAGEID=`docker image ls quant-toolbox -q`
docker container run --name quant-toolbox -p 8000:8000 -v /home/work/data/quant-toolbox/data:/app/data -e LXR_TOKEN -d $IMAGEID
echo "run new container done"
