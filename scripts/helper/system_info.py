import os
import platform

def print_system_info():
    """Prints detailed system information including CPU and OS details, as well as AVX2 and AVX-512 support."""
    print("========== System Information ==========")
    print(f"Operating System   : {platform.system()}")
    print(f"OS Release         : {platform.release()}")
    print(f"OS Version         : {platform.version()}")
    print(f"Platform           : {platform.platform()}")
    print(f"Machine            : {platform.machine()}")
    print(f"Processor          : {platform.processor()}")
    print(f"CPU Count          : {os.cpu_count()}")

    # Attempt to import py-cpuinfo for detailed CPU flags
    try:
        import cpuinfo
        cpu_info = cpuinfo.get_cpu_info()
        flags = cpu_info.get("flags", [])
        print("CPU Flags          :")
        print(f"  AVX2 support     : {'Yes' if 'avx2' in flags else 'No'}")
        # Check for AVX-512 features. Here we check for the AVX-512 Foundation flag ("avx512f")
        print(f"  AVX-512 support  : {'Yes' if 'avx512f' in flags else 'No'}")
    except ImportError:
        print("py-cpuinfo module not installed. Skipping advanced CPU flags check.")
        print("To enable advanced CPU info, install it via: pip install py-cpuinfo")

    print("========================================")


if __name__ == "__main__":
    print_system_info()
