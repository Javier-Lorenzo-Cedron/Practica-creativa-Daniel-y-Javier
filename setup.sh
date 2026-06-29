#!/usr/bin/env bash
set -euo pipefail

# ==========================================================================
# Script de preparación automática para la práctica creativa
# Ejecutar ANTES de "docker compose up -d"
# 
# IDEMPOTENTE: puede ejecutarse múltiples veces sin efectos adversos
# 
# Uso: chmod +x setup.sh && ./setup.sh
# ==========================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }
log_skip()    { echo -e "${GREEN}[SKIP]${NC} $1 (ya completado)"; }

detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if command -v apt-get &> /dev/null; then
            OS="debian"
        elif command -v dnf &> /dev/null; then
            OS="fedora"
        elif command -v yum &> /dev/null; then
            OS="centos"
        elif command -v pacman &> /dev/null; then
            OS="arch"
        else
            OS="linux-unknown"
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
    else
        OS="unknown"
    fi
    log_info "Sistema operativo detectado: $OS"
}

command_exists() {
    command -v "$1" &> /dev/null
}

# --------------------------------------------------------------------------
# Limpiar repositorios problemáticos (solo si existen)
# --------------------------------------------------------------------------
cleanup_problematic_repos() {
    local cleaned=false

    # Solo limpiar si existen archivos problemáticos
    if ls /etc/apt/sources.list.d/sbt*.list 1>/dev/null 2>&1; then
        log_info "Limpiando repositorios sbt antiguos..."
        sudo rm -f /etc/apt/sources.list.d/sbt*.list 2>/dev/null || true
        cleaned=true
    fi
    
    # Limpiar claves antiguas (silencioso si no existen)
    sudo apt-key del 99E82A75642AC823 2>/dev/null || true
    sudo apt-key del 2EE0EA64E40A89B84B2DF73499E82A75642AC823 2>/dev/null || true

    # Repositorio p4lang con clave expirada
    if grep -rq "p4lang" /etc/apt/sources.list.d/ 2>/dev/null; then
        log_warn "Detectado repositorio p4lang con clave expirada. Deshabilitando..."
        sudo rm -f /etc/apt/sources.list.d/*p4lang* 2>/dev/null || true
        cleaned=true
    fi

    if [[ "$cleaned" == true ]]; then
        log_success "Repositorios problemáticos limpiados"
    fi
}

# --------------------------------------------------------------------------
# Instalar Docker
# --------------------------------------------------------------------------
install_docker() {
    if command_exists docker; then
        log_skip "Docker ya instalado: $(docker --version | head -c 50)"
        return 0
    fi

    log_info "Instalando Docker..."
    
    case $OS in
        debian)
            sudo apt-get update
            sudo apt-get install -y ca-certificates curl gnupg
            sudo install -m 0755 -d /etc/apt/keyrings
            
            # Solo descargar clave si no existe
            if [[ ! -f /etc/apt/keyrings/docker.gpg ]]; then
                curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
                sudo chmod a+r /etc/apt/keyrings/docker.gpg
            fi
            
            # Solo añadir repo si no existe
            if [[ ! -f /etc/apt/sources.list.d/docker.list ]]; then
                echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            fi
            
            sudo apt-get update
            sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            ;;
        fedora)
            sudo dnf -y install dnf-plugins-core
            sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo 2>/dev/null || true
            sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            sudo systemctl start docker
            sudo systemctl enable docker
            ;;
        centos)
            sudo yum install -y yum-utils
            sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo 2>/dev/null || true
            sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            sudo systemctl start docker
            sudo systemctl enable docker
            ;;
        arch)
            sudo pacman -Sy --noconfirm --needed docker docker-compose
            sudo systemctl start docker
            sudo systemctl enable docker
            ;;
        macos)
            if command_exists brew; then
                brew install --cask docker 2>/dev/null || log_warn "Docker Desktop ya instalado o requiere intervención manual"
                log_warn "Asegúrate de que Docker Desktop esté ejecutándose antes de continuar."
                read -p "Presiona Enter cuando Docker Desktop esté listo..."
            else
                log_error "Instala Homebrew primero: https://brew.sh"
                exit 1
            fi
            ;;
        *)
            log_error "No se puede instalar Docker automáticamente en este sistema."
            exit 1
            ;;
    esac

    if [[ "$OS" != "macos" ]]; then
        if ! groups "$USER" | grep -q docker; then
            sudo usermod -aG docker "$USER" 2>/dev/null || true
            log_warn "Usuario añadido al grupo docker. Puede que necesites cerrar sesión y volver a entrar."
        fi
    fi

    log_success "Docker instalado correctamente"
}

# --------------------------------------------------------------------------
# Instalar Java 17
# --------------------------------------------------------------------------
install_java() {
    if command_exists java; then
        JAVA_VER=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
        if [[ "$JAVA_VER" -ge 17 ]]; then
            log_skip "Java 17+ ya instalado"
            return 0
        fi
    fi

    log_info "Instalando Java 17..."
    
    case $OS in
        debian)
            sudo apt-get update
            sudo apt-get install -y openjdk-17-jdk
            ;;
        fedora)
            sudo dnf install -y java-17-openjdk-devel
            ;;
        centos)
            sudo yum install -y java-17-openjdk-devel
            ;;
        arch)
            sudo pacman -Sy --noconfirm --needed jdk17-openjdk
            ;;
        macos)
            brew install openjdk@17 2>/dev/null || true
            sudo ln -sfn "$(brew --prefix openjdk@17)/libexec/openjdk.jdk" /Library/Java/JavaVirtualMachines/openjdk-17.jdk 2>/dev/null || true
            export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
            ;;
        *)
            log_error "Instala Java 17 manualmente."
            exit 1
            ;;
    esac

    log_success "Java 17 instalado correctamente"
}

# --------------------------------------------------------------------------
# Instalar sbt
# --------------------------------------------------------------------------
install_sbt() {
    if command_exists sbt; then
        log_skip "sbt ya instalado"
        return 0
    fi

    log_info "Instalando sbt..."
    
    case $OS in
        debian)
            sudo install -m 0755 -d /etc/apt/keyrings
            
            # Solo descargar clave si no existe
            if [[ ! -f /etc/apt/keyrings/sbt-archive-keyring.gpg ]]; then
                log_info "Descargando clave GPG de sbt..."
                curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
                    | sudo gpg --dearmor -o /etc/apt/keyrings/sbt-archive-keyring.gpg
            fi
            
            # Solo añadir repo si no existe (o recrear con formato correcto)
            echo "deb [signed-by=/etc/apt/keyrings/sbt-archive-keyring.gpg] https://repo.scala-sbt.org/scalasbt/debian all main" \
                | sudo tee /etc/apt/sources.list.d/sbt.list > /dev/null
            
            sudo apt-get update
            sudo apt-get install -y sbt
            ;;
        fedora|centos)
            if [[ ! -f /etc/yum.repos.d/sbt-rpm.repo ]]; then
                curl -L https://www.scala-sbt.org/sbt-rpm.repo | sudo tee /etc/yum.repos.d/sbt-rpm.repo
            fi
            sudo dnf install -y sbt 2>/dev/null || sudo yum install -y sbt
            ;;
        arch)
            sudo pacman -Sy --noconfirm --needed sbt
            ;;
        macos)
            brew install sbt 2>/dev/null || true
            ;;
        *)
            log_error "Instala sbt manualmente: https://www.scala-sbt.org/download.html"
            exit 1
            ;;
    esac

    log_success "sbt instalado correctamente"
}

# --------------------------------------------------------------------------
# Instalar Python 3 con venv
# --------------------------------------------------------------------------
install_python() {
    local need_venv=false
    
    if command_exists python3; then
        PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        log_skip "Python $PY_VER ya instalado"
    else
        log_info "Instalando Python 3..."
        case $OS in
            debian)
                sudo apt-get update
                sudo apt-get install -y python3 python3-pip
                ;;
            fedora)
                sudo dnf install -y python3 python3-pip
                ;;
            centos)
                sudo yum install -y python3 python3-pip
                ;;
            arch)
                sudo pacman -Sy --noconfirm --needed python python-pip
                ;;
            macos)
                brew install python@3.11 2>/dev/null || true
                ;;
            *)
                log_error "Instala Python 3.10+ manualmente."
                exit 1
                ;;
        esac
        log_success "Python instalado"
        PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    fi

    # Verificar si venv funciona
    if ! python3 -c "import venv; import ensurepip" 2>/dev/null; then
        need_venv=true
    fi

    # Instalar python3-venv si es necesario (Debian/Ubuntu)
    if [[ "$OS" == "debian" && "$need_venv" == true ]]; then
        log_info "Instalando python3-venv..."
        
        VENV_PKG="python${PY_VER}-venv"
        
        sudo apt-get update
        sudo apt-get install -y "$VENV_PKG" python3-venv python3-pip 2>/dev/null || {
            sudo apt-get install -y python3-venv python3-pip
        }
        
        log_success "python3-venv instalado"
    elif [[ "$need_venv" == false ]]; then
        log_skip "python3-venv ya disponible"
    fi
}

# --------------------------------------------------------------------------
# Instalar curl
# --------------------------------------------------------------------------
install_curl() {
    if command_exists curl; then
        log_skip "curl ya instalado"
        return 0
    fi

    log_info "Instalando curl..."
    case $OS in
        debian)
            sudo apt-get update && sudo apt-get install -y curl
            ;;
        fedora)
            sudo dnf install -y curl
            ;;
        centos)
            sudo yum install -y curl
            ;;
        arch)
            sudo pacman -Sy --noconfirm --needed curl
            ;;
        macos)
            brew install curl 2>/dev/null || true
            ;;
        *)
            log_error "Instala curl manualmente."
            exit 1
            ;;
    esac
    log_success "curl instalado"
}

# --------------------------------------------------------------------------
# Instalar git
# --------------------------------------------------------------------------
install_git() {
    if command_exists git; then
        return 0
    fi

    log_info "Instalando git..."
    case $OS in
        debian) sudo apt-get update && sudo apt-get install -y git ;;
        fedora) sudo dnf install -y git ;;
        centos) sudo yum install -y git ;;
        arch) sudo pacman -Sy --noconfirm --needed git ;;
        macos) brew install git 2>/dev/null || true ;;
    esac
}

# --------------------------------------------------------------------------
# Clonar repositorio (o entrar si ya existe)
# --------------------------------------------------------------------------
clone_repo() {
    REPO_URL="https://github.com/Javier-Lorenzo-Cedron/Practica-creativa-Daniel-y-Javier.git"
    REPO_DIR="Practica-creativa-Daniel-y-Javier"

    # Ya estamos en el directorio del proyecto
    if [[ -f "docker-compose.yml" || -f "docker-compose.yaml" ]]; then
        log_skip "Ya estás en el directorio del proyecto"
        return 0
    fi

    # El directorio existe, entramos
    if [[ -d "$REPO_DIR" ]]; then
        log_skip "Repositorio ya clonado"
        cd "$REPO_DIR"
        return 0
    fi

    # Clonar
    install_git
    log_info "Clonando repositorio..."
    git clone "$REPO_URL"
    cd "$REPO_DIR"
    log_success "Repositorio clonado"
}

# --------------------------------------------------------------------------
# Configurar entorno virtual de Python
# --------------------------------------------------------------------------
setup_python_env() {
    log_info "Verificando entorno virtual de Python..."

    if [[ ! -f "requirements.txt" ]]; then
        log_error "No se encuentra requirements.txt. ¿Estás en el directorio correcto?"
        exit 1
    fi

    # Verificar si el venv existe y es funcional
    if [[ -d "env" && -f "env/bin/activate" && -f "env/bin/python" ]]; then
        # Verificar que el venv funciona
        if env/bin/python -c "import pip" 2>/dev/null; then
            # Verificar si las dependencias ya están instaladas
            source env/bin/activate
            if pip freeze | grep -qi "pyspark"; then
                log_skip "Entorno virtual ya configurado con dependencias"
                return 0
            fi
            # Dependencias no instaladas, instalarlas
            log_info "Instalando dependencias faltantes..."
            pip install --upgrade pip
            pip install -r requirements.txt
            log_success "Dependencias instaladas"
            return 0
        fi
    fi

    # Eliminar venv corrupto si existe
    if [[ -d "env" ]]; then
        log_warn "Entorno virtual corrupto detectado, recreando..."
        rm -rf env
    fi

    # Crear venv nuevo
    log_info "Creando entorno virtual..."
    python3 -m venv env
    
    if [[ ! -f "env/bin/activate" ]]; then
        log_error "Error al crear el entorno virtual"
        exit 1
    fi

    source env/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    log_success "Entorno virtual creado y dependencias instaladas"
}

# --------------------------------------------------------------------------
# Descargar datos
# --------------------------------------------------------------------------
download_data() {
    log_info "Verificando datos..."

    # Verificar si los datos ya existen
    if [[ -f "data/simple_flight_delay_features.jsonl.bz2" && -f "data/origin_dest_distances.jsonl" ]]; then
        log_skip "Datos ya descargados en data/"
        return 0
    fi

    if [[ ! -f "resources/download_data.sh" ]]; then
        log_error "No se encuentra resources/download_data.sh"
        exit 1
    fi

    log_info "Descargando datos..."
    chmod +x resources/download_data.sh
    bash resources/download_data.sh

    if [[ -f "data/simple_flight_delay_features.jsonl.bz2" && -f "data/origin_dest_distances.jsonl" ]]; then
        log_success "Datos descargados correctamente"
    else
        log_error "Error al descargar los datos"
        exit 1
    fi
}

# --------------------------------------------------------------------------
# Compilar JAR de Scala
# --------------------------------------------------------------------------
compile_jar() {
    JAR_PATH="flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"

    # Verificar si el JAR ya existe
    if [[ -f "$JAR_PATH" ]]; then
        log_skip "JAR ya compilado: $JAR_PATH"
        return 0
    fi

    if [[ ! -d "flight_prediction" ]]; then
        log_error "No se encuentra la carpeta flight_prediction/"
        exit 1
    fi

    log_info "Compilando job de Scala..."
    cd flight_prediction
    sbt package
    cd ..

    if [[ -f "$JAR_PATH" ]]; then
        log_success "JAR compilado correctamente"
    else
        log_error "Error al compilar el JAR"
        exit 1
    fi
}

# --------------------------------------------------------------------------
# Verificación final
# --------------------------------------------------------------------------
final_check() {
    echo ""
    echo "=========================================================================="
    echo -e "${GREEN}✓ PREPARACIÓN COMPLETADA${NC}"
    echo "=========================================================================="
    echo ""
    echo "Resumen de verificación:"
    echo ""
    
    command_exists docker && echo -e "  ${GREEN}✓${NC} Docker" || echo -e "  ${RED}✗${NC} Docker"
    command_exists java && echo -e "  ${GREEN}✓${NC} Java" || echo -e "  ${RED}✗${NC} Java"
    command_exists sbt && echo -e "  ${GREEN}✓${NC} sbt" || echo -e "  ${RED}✗${NC} sbt"
    command_exists python3 && echo -e "  ${GREEN}✓${NC} Python" || echo -e "  ${RED}✗${NC} Python"
    command_exists curl && echo -e "  ${GREEN}✓${NC} curl" || echo -e "  ${RED}✗${NC} curl"
    [[ -d "env" && -f "env/bin/activate" ]] && echo -e "  ${GREEN}✓${NC} Entorno virtual Python" || echo -e "  ${RED}✗${NC} Entorno virtual"
    [[ -f "data/simple_flight_delay_features.jsonl.bz2" ]] && echo -e "  ${GREEN}✓${NC} Datos descargados" || echo -e "  ${RED}✗${NC} Datos"
    [[ -f "flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar" ]] && echo -e "  ${GREEN}✓${NC} JAR compilado" || echo -e "  ${RED}✗${NC} JAR"

    echo ""
    echo "=========================================================================="
    echo -e "${YELLOW}SIGUIENTE PASO:${NC}"
    echo ""
    echo "  Continúa con el README desde el paso 4:"
    echo ""
    echo -e "  ${BLUE}docker compose up -d${NC}"
    echo ""
    echo "=========================================================================="
}

# ==========================================================================
# MAIN
# ==========================================================================
main() {
    echo ""
    echo "=========================================================================="
    echo "  SCRIPT DE PREPARACIÓN AUTOMÁTICA - Práctica Creativa"
    echo "=========================================================================="
    echo ""

    detect_os

    if [[ "$OS" == "unknown" || "$OS" == "linux-unknown" ]]; then
        log_error "Sistema operativo no soportado para instalación automática."
        exit 1
    fi

    # Limpiar repos problemáticos solo en Debian/Ubuntu
    if [[ "$OS" == "debian" ]]; then
        cleanup_problematic_repos
    fi

    log_info "=== Paso 0: Verificando dependencias del sistema ==="
    install_curl
    install_docker
    install_java
    install_sbt
    install_python

    log_info "=== Verificando repositorio ==="
    clone_repo

    log_info "=== Paso 1: Entorno Python ==="
    setup_python_env

    log_info "=== Paso 2: Datos ==="
    download_data

    log_info "=== Paso 3: Compilación JAR ==="
    compile_jar

    final_check
}

main "$@"
