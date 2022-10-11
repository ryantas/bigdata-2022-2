# from dockerfile create an image
docker build -t raytest .
# create a container from image
docker run -d --rm --name myray -p 8888:8888 raytest
# docker run --help