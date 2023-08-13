# 停止和删除旧容器
OLD_CONTAINER_ID=`docker container ls -aq --filter name=quant-toolbox`
if [ -n $OLD_CONTAINER_ID ];then
    docker container stop $OLD_CONTAINER_ID
    docker container rm $OLD_CONTAINER_ID
fi
echo "stop and rm old container success!"


# 删除旧镜像
OLD_IMAGE_ID=`docker image ls quant-toolbox -q`
if [ -n $OLD_IMAGE_ID ]; then
    docker image rm $OLD_IMAGE_ID
fi

echo "rm old image success!"

# 加载新镜像
docker image load -i quant-toolbox.tar.gz
echo "load new container success!"

# 启用新容器
IMAGEID=`docker image ls quant-toolbox -q`
docker container run --name quant-toolbox -p 8000:8000 -v /home/work/online/quant-toolbox/data:/app/data -v /home/work/online/quant-toolbox/logs:/app/logs -e LXR_TOKEN -e DB_HOST -e DB_PORT -e DB_USER -e DB_PASSWORD -d $IMAGEID
echo "run new container success"
