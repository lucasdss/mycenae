#------------------------------------------------------------------------------
# P A C K A G E  I N F O
#------------------------------------------------------------------------------
%define binaryname mycenae
%define projectname mycenae
Name:      %{projectname}
Version:   3.0.build_time
Release:   1

Packager:  UOL - Universo Online S.A.
Vendor:    UOL - Universo Online S.A.
URL:       https://www.uol.com.br

Source0:   https://github.com/uol/mycenae
License:   GPLv3

BuildRoot: %{_tmppath}-%{name}-%{version}
BuildArch: x86_64

Summary:   Mycenae timeseries database that uses cassandra and elasticsearch as backends
Group:     Application/Internet

%description
Mycenae is a timeseries database that uses cassandra and elasticsearch as backends.

#------------------------------------------------------------------------------
# B U I L D
#------------------------------------------------------------------------------
%prep

%build
cd ${GOPATH}/src/github.com/uol/%{projectname}
make build


#------------------------------------------------------------------------------
# I N S T A L L  F I L E S
#------------------------------------------------------------------------------
%install
buildtmp=${GOPATH}/src/github.com/uol/%{projectname}
rm -rf "%{buildroot}"
install -m 755 -d "%{buildroot}"/opt/%{projectname}/bin
install -m 755 ${buildtmp}/%{binaryname} "%{buildroot}"/opt/%{projectname}/bin
install -m 755 -d "%{buildroot}"/var/log/%{projectname}
install -m 755 -d "%{buildroot}"/etc/logrotate.d
install -m 644 ${buildtmp}/rpm/%{binaryname}.logrotate "%{buildroot}"/etc/logrotate.d/%{projectname}
install -m 755 -d "%{buildroot}"/etc/rc.d/init.d
install -m 755 ${buildtmp}/rpm/%{binaryname}.initd "%{buildroot}"/etc/rc.d/init.d/%{projectname}
install -m 755 -d "%{buildroot}"/lib/systemd/system
install -m 755 ${buildtmp}/rpm/%{projectname}.service "%{buildroot}"/lib/systemd/system/%{projectname}.service
install -m 755 -d "%{buildroot}"/var/run/%{projectname}
install -m 755 -d "%{buildroot}"/var/lib/%{projectname}
install -m 755 -d "%{buildroot}"/usr/local/bin
install -m 755 ${buildtmp}/rpm/%{binaryname}Start.sh "%{buildroot}"/usr/local/bin
install -m 755 ${buildtmp}/rpm/%{binaryname}Stop.sh "%{buildroot}"/usr/local/bin

#------------------------------------------------------------------------------
# P R E - I N S T A L L  S C R I P T
#------------------------------------------------------------------------------
%pre

if [ "$( getent group "%{binaryname}" )" == "" ]; then
    groupadd -r "%{binaryname}"
fi

if [ "$( getent passwd "%{binaryname}" )" == "" ]; then
    useradd -d "/opt/%{projectname}" -c "User of mycenae application" \
        -g "mycenae" -s "/sbin/nologin" -r \
        "%{binaryname}"
fi

#------------------------------------------------------------------------------
# P O S T - I N S T A L L  S C R I P T
#------------------------------------------------------------------------------
%post
if grep -q " 7" /etc/redhat-release
then
	systemctl mycenae
	systemctl daemon-reload
else
	chkconfig --add mycenae
	chkconfig mycenae on
fi

#------------------------------------------------------------------------------
# P R E - U N I N S T A L L  S C R I P T
#------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------
# P O S T - U N I N S T A L L  S C R I P T
#------------------------------------------------------------------------------
%postun

#------------------------------------------------------------------------------
# C L E A N  U P
#------------------------------------------------------------------------------
%clean
/bin/rm -rf "%{buildroot}"

#------------------------------------------------------------------------------
# F I L E S
#------------------------------------------------------------------------------
%files
%attr(0755, root, root) /opt/%{projectname}/bin/%{binaryname}
%attr(0755, root, root) /etc/rc.d/init.d/%{projectname}
%attr(0644, root, root) /etc/logrotate.d/%{projectname}
%dir /var/log/%{projectname}
%attr(0755, %{binaryname}, %{binaryname}) /var/log/%{projectname}
%dir /var/run/%{projectname}
%attr(0755, %{binaryname}, root) /var/run/%{projectname}
%dir /lib/systemd/system
%attr(0755, root, root) /lib/systemd/system/%{projectname}.service
%dir /var/lib/%{projectname}
%attr(0755, %{binaryname}, %{binaryname}) /var/lib/%{projectname}
%attr(0755, root, root) /usr/local/bin/%{binaryname}Start.sh
%attr(0755, root, root) /usr/local/bin/%{binaryname}Stop.sh

#------------------------------------------------------------------------------
# C H A N G E L O G
#------------------------------------------------------------------------------
%changelog
