#
# Docker to run mycenae
#
FROM ubuntu:xenial
MAINTAINER José David Fernández Curado

#
# Files
#
COPY docker.local.toml "/config/mycenae.toml"
COPY mycenae "/bin/mycenae"

#
# Start
#
#CMD /bin/mycenae -config=/config/mycenae.toml || find /tmp -iname '*.log' | xargs cat
CMD [ "/bin/sh", "-c", "/bin/mycenae -config=/config/mycenae.toml" ]
