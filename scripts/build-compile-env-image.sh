#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# omnistream is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly DOCKERFILE="${SCRIPT_DIR}/Dockerfile"
readonly BUILD_PARALLELISM="${BUILD_PARALLELISM:-32}"

# Maps an "<os>/<architecture>" target to its local base-image tag and archive URL.
declare -A BASE_IMAGES=(
    ["openeuler-22.03-lts-sp4/aarch64"]="openeuler-22.03-lts-sp4"
    ["openeuler-24.03-lts-sp4/aarch64"]="openeuler-24.03-lts-sp4"
)

declare -A BASE_IMAGE_URLS=(
    ["openeuler-22.03-lts-sp4/aarch64"]="https://mirrors.huaweicloud.com/openeuler/openEuler-22.03-LTS-SP4/docker_img/aarch64/openEuler-docker.aarch64.tar.xz"
    ["openeuler-24.03-lts-sp4/aarch64"]="https://mirrors.huaweicloud.com/openeuler/openEuler-24.03-LTS-SP4/docker_img/aarch64/openEuler-docker.aarch64.tar.xz"
)

usage() {
    cat <<'EOF'
Usage: bash scripts/build-compile-env-image.sh <os> <architecture> [options]

Supported targets:
EOF
    while IFS=/ read -r os architecture; do
        printf '  %s %s\n' "${os}" "${architecture}"
    done < <(printf '%s\n' "${!BASE_IMAGES[@]}" | sort)
    cat <<'EOF'

Options:
  -p, --proxy <url>       HTTP/HTTPS proxy used to download resources and build the image
  -t, --tag <image>       Image name and tag (default: omnistream-build-env:<os>-<architecture>-<YYYYMMDD>)
  -h, --help              Show this help message

Example:
  bash scripts/build-compile-env-image.sh openeuler-22.03-lts-sp4 aarch64 --proxy http://proxy.example.com:8080
EOF
}

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
    usage
    exit 0
fi

[[ $# -ge 2 ]] || { usage >&2; exit 1; }
readonly TARGET_OS="$1"
readonly TARGET_ARCH="$2"
shift 2

if [[ ! "${TARGET_OS}" =~ ^[[:alnum:]._-]+$ || ! "${TARGET_ARCH}" =~ ^[[:alnum:]._-]+$ ]]; then
    echo "OS and architecture may contain only letters, numbers, dots, underscores, and hyphens." >&2
    exit 1
fi

PROXY_URL=""
IMAGE_NAME=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--proxy)
            [[ $# -ge 2 ]] || { echo "Missing value for $1." >&2; exit 1; }
            PROXY_URL="$2"
            shift 2
            ;;
        -t|--tag)
            [[ $# -ge 2 ]] || { echo "Missing value for $1." >&2; exit 1; }
            IMAGE_TAG="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

[[ -f "${DOCKERFILE}" ]] || { echo "Dockerfile not found: ${DOCKERFILE}" >&2; exit 1; }

readonly TARGET_KEY="${TARGET_OS}/${TARGET_ARCH}"
if [[ -z "${BASE_IMAGES[${TARGET_KEY}]+_}" || -z "${BASE_IMAGE_URLS[${TARGET_KEY}]+_}" ]]; then
    echo "Unsupported OS and architecture: ${TARGET_KEY}" >&2
    exit 1
fi
readonly BASE_IMAGE="${BASE_IMAGES[${TARGET_KEY}]}"
readonly BASE_IMAGE_URL="${BASE_IMAGE_URLS[${TARGET_KEY}]}"

readonly BUILD_DATE="$(date +%Y%m%d)"
readonly IMAGE_TAG="${IMAGE_TAG:-omnistream-compile-env:${TARGET_OS}-${TARGET_ARCH}-${BUILD_DATE}}"

download() {
    if [[ -n "${PROXY_URL}" ]]; then
        http_proxy="${PROXY_URL}" https_proxy="${PROXY_URL}" \
            wget --no-check-certificate -O "$1" "$2"
    else
        wget --no-check-certificate -O "$1" "$2"
    fi
}

base_image_archive=""
trap 'rm -f "${base_image_archive}"' EXIT

if ! docker image inspect "${BASE_IMAGE}" >/dev/null 2>&1; then
    base_image_archive="$(mktemp --suffix=.tar.xz)"
    echo "Base image ${BASE_IMAGE} was not found; downloading it..."
    download "${base_image_archive}" "${BASE_IMAGE_URL}"
    docker load -i "${base_image_archive}"
fi

docker_build_args=(
    --file "${DOCKERFILE}"
    --build-arg "BASE_IMAGE=${BASE_IMAGE}"
    --build-arg "BUILD_PARALLELISM=${BUILD_PARALLELISM}"
    --tag "${IMAGE_TAG}"
)
if [[ -n "${PROXY_URL}" ]]; then
    docker_build_args+=(
        --build-arg "HTTP_PROXY=${PROXY_URL}"
        --build-arg "HTTPS_PROXY=${PROXY_URL}"
        --build-arg "http_proxy=${PROXY_URL}"
        --build-arg "https_proxy=${PROXY_URL}"
    )
fi

echo "Building ${IMAGE_TAG} from ${BASE_IMAGE}..."
docker build "${docker_build_args[@]}" "${SCRIPT_DIR}"
