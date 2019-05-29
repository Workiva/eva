FROM mysql:5.7

COPY ./init.sql /docker-entrypoint-initdb.d/init-eva-db.sql
ENTRYPOINT ["docker-entrypoint.sh"]
EXPOSE 3306
CMD ["mysqld"]
