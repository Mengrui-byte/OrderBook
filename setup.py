from setuptools import setup
from pybind11.setup_helpers import Pybind11Extension, build_ext

ext_modules = [
    Pybind11Extension(
        "orderbook._cpp",
        ["src/bindings.cpp"],
        depends=["include/orderbook.hpp", "include/state_machine.hpp", "include/checkpoint.hpp"],
        include_dirs=["include"],
        cxx_std=17,
        extra_compile_args=["-O3", "-march=native", "-DNDEBUG", "-flto", "-funroll-loops"],
        extra_link_args=["-flto"],
    ),
]

setup(
    name="orderbook",
    version="0.2.0",
    description="High-performance order book based on absl::btree_map",
    packages=["orderbook"],
    package_data={"orderbook": ["config.ini"]},
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    extras_require={
        "fast-io": ["pyarrow>=14.0"],  # 可选：CSV 读取约 5–10x 加速
    },
)
