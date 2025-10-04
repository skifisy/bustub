#!/bin/bash

# 批量编译并运行脚本
# 直接在数组中定义要编译和运行的目标

# 设置并行编译的线程数
JOBS=32

# 在这里定义要编译的目标数组
TARGETS=(
    "b_plus_tree_insert_test"
    "b_plus_tree_delete_test" 
    "b_plus_tree_concurrent_test"
)

# 检查目标数组是否为空
if [ ${#TARGETS[@]} -eq 0 ]; then
    echo "错误: 目标数组为空，请添加编译目标"
    exit 1
fi

echo "要编译的目标: ${TARGETS[*]}"
echo "开始并行编译 (使用 -j$JOBS)..."

# 编译所有目标
for target in "${TARGETS[@]}"; do
    echo "正在编译: $target"
    make -j$JOBS $target
    if [ $? -ne 0 ]; then
        echo "错误: 编译目标 $target 失败"
        exit 1
    fi
done

echo "所有目标编译完成 ✓"
echo "开始运行目标..."

# 运行所有目标
for target in "${TARGETS[@]}"; do
    # 检查可执行文件是否存在
    if [ -f "$target" ]; then
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "运行: $target"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ./$target
        echo ""
    elif [ -f "bin/$target" ]; then
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "运行: bin/$target"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ./bin/$target
        echo ""
    elif [ -f "test/$target" ]; then
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "运行: test/$target"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ./test/$target
        echo ""
    else
        echo "警告: 找不到可执行文件 $target (检查 ./$target 或 ./bin/$target 或 ./test/$target)"
    fi
done

echo "所有操作完成 ✓"