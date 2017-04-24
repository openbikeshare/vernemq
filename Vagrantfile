# _*_ mode: ruby _*_
# vi: set ft=ruby :

$script = <<SCRIPT

MAKE=make

case $2 in
"apt")
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get -y upgrade
    if [ "$1" = "precise" ]; then
        # precise comes with a too old version of git, not compatible with rebar3
        sudo apt-get install -y software-properties-common python-software-properties
        sudo add-apt-repository -y ppa:git-core/ppa
        # precise comes with a too old C++ compiler, not compatible with mzmetrics
        sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
        sudo apt-get update
        sudo apt-get -y install build-essential gcc-4.8 g++-4.8
        sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.8 50
        sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.8 50
    fi
    sudo apt-get -y install curl build-essential git packaging-dev libssl-dev openssl libncurses5-dev
    ;;  
"yum")
    sudo yum -y update
    sudo yum -y groupinstall 'Development Tools'
    sudo yum -y install curl git ncurses-devel openssl openssl-devel
    ;;
"pkg")
    sudo pkg install -y curl gmake gcc bash editors/emacs-nox11 git
    export CC=clang CXX=clang CFLAGS="-g -O3 -fstack-protector" LDFLAGS="-fstack-protector"
    # compilation options
    echo 'KERL_CONFIGURE_OPTIONS="--disable-native-libs --enable-vm-probes --with-dynamic-trace=dtrace --with-ssl=/usr/local --enable-kernel-poll --without-odbc --enable-threads --enable-sctp --enable-smp-support"' > ~/.kerlrc

    # Make sure the machine has a hostname
    if ! grep -Fxq "hostname=freebsd" /etc/rc.conf
    then
        sudo sh -c $(echo hostname="freebsd" >> /etc/rc.conf)
        sudo hostname freebsd
    fi

    MAKE=gmake
    ;;
esac

    curl -O https://raw.githubusercontent.com/yrashk/kerl/master/kerl
    chmod a+x kerl
    ./kerl update releases
    ./kerl build $4 $4
    mkdir -p erlang-$4
    ./kerl install $4 erlang-$4/
    . erlang-$4/activate
    
    if cd vernemq; then 
        git checkout master
        git pull
    else 
        git clone git://github.com/erlio/vernemq vernemq 
        cd vernemq
    fi
    git checkout $3

    $MAKE rel
SCRIPT

$vernemq_release = '1.0.0'
$erlang_release = '19.2'

$configs = {
    :jessie => {:sys => :apt, :img => 'debian/jessie64'},
    :wheezy => {:sys => :apt, :img => 'debian/wheezy64'},
    :trusty => {:sys => :apt, :img => 'ubuntu/trusty64', :primary => true},
    :precise => {:sys => :apt, :img => 'ubuntu/precise64'},
    :centos7 => {:sys => :yum, :img => 'puppetlabs/centos-7.0-64-nocm'},
    :xenial => {:sys => :apt, :img => 'ubuntu/xenial64'},
    :freebsd => {:sys => :pkg, :img => 'freebsd/FreeBSD-11.0-STABLE', :shell => "/bin/sh"},
}

Vagrant.configure(2) do |config|
    $configs.each do |dist, dist_config|
        config.vm.define dist, 
            primary: dist_config[:primary],
            autostart: dist_config[:primary] do |c|
            c.vm.box = dist_config[:img]
            c.ssh.shell = dist_config[:shell] || "bash"
            c.vm.synced_folder ".", "/vagrant", type: "rsync"
            c.vm.provision :shell do |s|
                s.privileged = false
                s.inline = $script
                s.args = ["#{dist}", "#{dist_config[:sys]}", $vernemq_release, $erlang_release]
            end
        end
    end
end
