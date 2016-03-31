%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}

# distribution specific definitions
%define use_systemd (0%{?fedora} && 0%{?fedora} >= 18) || (0%{?rhel} && 0%{?rhel} >= 7) || (0%{?suse_version} == 1315)

%define nginx_home %{_localstatedir}/cache/nginx
%define nginx_user nginx
%define nginx_group nginx

Summary: NGINX Amplify Agent
Name: nginx-amplify-agent
Version: %{amplify_version}
Release: %{amplify_release}%{?dist}
Vendor: Nginx Software, Inc.
Packager: Nginx Software, Inc. <https://www.nginx.com>
Group: System Environment/Daemons
URL: https://github.com/nginxinc
License: 2-clause BSD-like license


Source0:   nginx-amplify-agent-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

Requires: python >= 2.6
Requires: initscripts >= 8.36
Requires(post): chkconfig


%description
The NGINX Amplify Agent is a small, Python application that
provides system and NGINX metric collection. It is part of
NGINX Amplify - the monitoring and configuration assistance
service for NGINX.
This package installs and runs NGINX Amplify Agent daemon.
See http://nginx.com/amplify for more information


%prep
%setup -q -n nginx-amplify-agent-%{version}
cp -p %{SOURCE0} .


%build
%{__python} -c 'import setuptools; execfile("setup.py")' build


%pre
# Add the "nginx" user
getent group %{nginx_group} >/dev/null || groupadd -r %{nginx_group}
getent passwd %{nginx_user} >/dev/null || \
    useradd -r -g %{nginx_group} -s /sbin/nologin \
    -d %{nginx_home} -c "nginx user"  %{nginx_user}
exit 0



%install
%define python_libexec /usr/bin/
[ "%{buildroot}" != "/" ] && rm -rf %{buildroot}
%{__python} -c 'import setuptools; execfile("setup.py")' install -O1 --skip-build --install-scripts %{python_libexec} --root %{buildroot}
mkdir -p %{buildroot}/var/
mkdir -p %{buildroot}/var/log/
mkdir -p %{buildroot}/var/log/amplify-agent/
mkdir -p %{buildroot}/var/
mkdir -p %{buildroot}/var/run/
mkdir -p %{buildroot}/var/run/amplify-agent/


%clean
[ "%{buildroot}" != "/" ] && rm -rf %{buildroot}


%files
%define config_files /etc/amplify-agent/
%defattr(-,root,root,-)
%{python_sitelib}/*
%{python_libexec}/*
%{config_files}/*
%attr(0755,nginx,nginx) %dir /var/log/amplify-agent
%attr(0755,nginx,nginx) %dir /var/run/amplify-agent
/etc/init.d/amplify-agent
/etc/logrotate.d/amplify-agent




%post
if [ $1 -eq 1 ] ; then
%if %{use_systemd}
    /usr/bin/systemctl preset amplify-agent.service >/dev/null 2>&1 ||:
%else
    /sbin/chkconfig --add amplify-agent
%endif
    mkdir -p /var/run/amplify-agent
    touch /var/log/amplify-agent/agent.log
    chown nginx /var/run/amplify-agent /var/log/amplify-agent/agent.log
elif [ $1 -eq 2 ] ; then
    %define agent_conf_file /etc/amplify-agent/agent.conf

    # Check for an older version of the agent running
    if command -V pgrep > /dev/null 2>&1; then
        agent_pid=`pgrep amplify-agent`
    else
        agent_pid=`ps aux | grep -i '[a]mplify-agent' | awk '{print $2}'`
    fi

    # stop it
    if [ -n "$agent_pid" ]; then
        service amplify-agent stop > /dev/null 2>&1 < /dev/null
    fi

    # Change API URL to 1.1
    if [ -f "%{agent_conf_file}" ]; then
	    sh -c "sed -i.old 's/api_url.*$/api_url = https:\/\/receiver.amplify.nginx.com:443\/1.1/' \
	    %{agent_conf_file}"
    else
        test -f "%{agent_conf_file}.default" && \
        cp -p "%{agent_conf_file}.default" "%{agent_conf_file}" && \
        chmod 644 %{agent_conf_file} && \
	    chown nginx %{agent_conf_file} > /dev/null 2>&1
    fi

    # start it
    service amplify-agent start > /dev/null 2>&1 < /dev/null
fi

%preun
if [ $1 -eq 0 ]; then
%if %use_systemd
    /usr/bin/systemctl --no-reload disable amplify-agent.service >/dev/null 2>&1 ||:
    /usr/bin/systemctl stop amplify-agent.service >/dev/null 2>&1 ||:
%else
    /sbin/service amplify-agent stop > /dev/null 2>&1
    /sbin/chkconfig --del amplify-agent
%endif
fi



%changelog
* Thu Mar 31 2016 Mike Belov <dedm@nginx.com> 0.31-1
- 0.31-1
- Bug fixes

* Tue Mar 15 2016 Mike Belov <dedm@nginx.com> 0.30-1
- 0.30-1
- Bug fixes
- Initial SSL analytics support

* Tue Jan 19 2016 Mike Belov <dedm@nginx.com> 0.28-1
- 0.28-1
- Bug fixes
- Amazon Linux support
- Initial NGINX+ extended status support

* Thu Dec 17 2015 Mike Belov <dedm@nginx.com> 0.27-1
- 0.27-1
- Bug fixes

* Wed Dec 3 2015 Mike Belov <dedm@nginx.com> 0.25-1
- 0.25-1
- Bug fixes
- New metric: system.cpu.stolen
- Nginx config parsing improved

* Tue Nov 24 2015 Mike Belov <dedm@nginx.com> 0.24-2
- 0.24-2
- Bug fixes

* Tue Nov 24 2015 Mike Belov <dedm@nginx.com> 0.24-1
- 0.24-1
- Bug fixes

* Wed Nov 18 2015 Mike Belov <dedm@nginx.com> 0.23-1
- 0.23-1
- Bug fixes
- Ubuntu Wily support

* Sun Nov 15 2015 Mike Belov <dedm@nginx.com> 0.22-5
- 0.22-5
- Bug fixes

* Fri Nov 13 2015 Mike Belov <dedm@nginx.com> 0.22-4
- 0.22-4
- Bug fixes

* Thu Nov 12 2015 Mike Belov <dedm@nginx.com> 0.22-3
- 0.22-3
- Bug fixes

* Wed Nov 11 2015 Mike Belov <dedm@nginx.com> 0.22-2
- 0.22-2
- Bug fixes

* Mon Nov 09 2015 Mike Belov <dedm@nginx.com> 0.22-1
- 0.22-1
- Bug fixes

* Thu Nov 05 2015 Mike Belov <dedm@nginx.com> 0.21-3
- 0.21-3
- Additional events added

* Wed Nov 04 2015 Mike Belov <dedm@nginx.com> 0.21-2
- 0.21-2
- Bug fixes

* Mon Nov 02 2015 Mike Belov <dedm@nginx.com> 0.21-1
- 0.21-1
- Bug fixes

* Wed Oct 28 2015 Mike Belov <dedm@nginx.com> 0.20-1
- 0.20-1
- RPM support
