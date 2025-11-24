FROM alpine:latest          
# Берем очень легкий Linux дистрибутив (Alpine)

# Устанавливаем bash (для скрипта) и util-linux (там находится утилита flock)
RUN apk add --no-cache bash util-linux 

COPY script.sh /entrypoint.sh 
# Копируем наш скрипт внутрь образа
RUN chmod +x /entrypoint.sh   
# Делаем его исполняемым

CMD ["/entrypoint.sh"]
# Команда, которая выполнится при старте контейнера