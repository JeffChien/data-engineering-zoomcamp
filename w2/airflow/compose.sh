PROJCT_NAME="dtde"
COMPOSE_FILE_W1='../../w1/docker-compose.yml'
COMPOSE_FILE_W2='./docker-compose.yml'

exec docker compose --env-file .env \
    -p ${PROJCT_NAME} \
    --project-directory `pwd` \
    -f ${COMPOSE_FILE_W1} -f ${COMPOSE_FILE_W2} $@