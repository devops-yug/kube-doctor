FROM nginxinc/nginx-unprivileged:1.27-alpine

COPY console-extensions.json /usr/share/nginx/html/console-extensions.json
COPY package.json /usr/share/nginx/html/plugin-manifest.json
COPY dist/ /usr/share/nginx/html/
COPY nginx.conf /etc/nginx/conf.d/default.conf

USER 1001
EXPOSE 9443
CMD ["nginx", "-g", "daemon off;"]
