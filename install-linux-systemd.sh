#!/bin/bash

#create tile38 user
useradd -m -d /var/lib/tile38 -s /sbin/nologin -U tile38

#copy binary file to bin path
cp -r tile38-server tile38-cli tile38-benchmark    /usr/bin/

#create PID dir
mkdir -p /run/tile38
chown -R tile38:tile38  /run/tile38
chmod -R 755 /run/tile38

#create PID file
touch /run/tile38/tile38.pid
chown -R tile38:tile38  /run/tile38/tile38.pid
chmod -R 664 /run/tile38/tile38.pid

#create systemd file
cat > /etc/systemd/system/tile38.service << 'EOF'
[Unit]
Description= Tile38 is a geospatial database, spatial index, and realtime geofence
Requires=network.target
After=network.target

[Service]
Type=simple
User=tile38
Group=tile38
ExecStart=/usr/bin/tile38-server -p 9851 -d /var/lib/tile38 --pidfile /run/tile38/tile38.pid
ExecStop=/bin/kill -SIGTERM ${MAINPID}

[Install]
WantedBy=multi-user.target
EOF

#reload units
systemctl daemon-reload

echo ""
echo "Installation tile38 completed."
echo ""
echo "Data path is : /var/lib/tile38"
echo "Port is : 9851"
echo ""
echo "For start tile38 service run this command :"
echo ""
echo "systemctl start tile38.service"
echo ""
