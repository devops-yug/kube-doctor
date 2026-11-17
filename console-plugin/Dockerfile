FROM nginxinc/nginx-unprivileged:1.27-alpine

USER root
COPY console-extensions.json /usr/share/nginx/html/console-extensions.json
COPY dist/plugin-manifest.json /usr/share/nginx/html/plugin-manifest.json
COPY dist/plugin-entry.js /usr/share/nginx/html/plugin-entry.js
COPY nginx.conf /etc/nginx/conf.d/default.conf
RUN chmod -R g+rwX /var/cache/nginx /var/run /run /etc/nginx/conf.d /usr/share/nginx/html

USER 1001
EXPOSE 9443
CMD ["nginx", "-g", "daemon off;"]
