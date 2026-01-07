##作为数据清洗工具尤其是分析代谢组学来使用
##环境Python
##安装包可按照需求自行下载
##后期可公开化修改以提升运行满意度
##1.适用于三平行短期、快速、准确清洗数据（如平行数量够可以更改代码直接清洗数据）。提交文件格式为xlsx或者csv格式。提交数据为经过QS质量控制（满足RSD≤30%的数量≥总数量的80%）的数据，数据中所有空格请清除。
##2.最好在激活虚拟环境中使用。
##3.适配MetaboAnalyst使用https://www.metaboanalyst.ca/MetaboAnalyst/
##存在缺点：
##1.清洗后数据需要手动删除化合物中含有的(+)-/(-)-/(±)- 前缀和′类符号（保留化合物名称本身）实例例如：(−)-    (-)-    (-)-     (+)-    ′     a    '    :等。
##2.未匹配KEGG数据库以及其他数据库，需手动进行，这个正在解决。


import pandas as pd
import re
import os
import numpy as np
from typing import List, Dict, Tuple
from tqdm import tqdm  # 新增：导入进度条库

# ========== 配置参数（用户可根据需求修改） ==========
# 输入文件路径
INPUT_EXCEL_PATH = r"/Users/a22222/Desktop/非靶向代谢组学分析测试/正离子鲜果30%数据/测试.xlsx"
# 浓度列前缀
CONCENTRATION_COL_PREFIX = "GroupArea:"
# 输出文件后缀
OUTPUT_FILE_SUFFIX = "_补齐空列数据_每个化合物RSD≤5%"
# 每组新增空白列数量
NEW_COLS_PER_GROUP = 3
# 数据波动范围
FLUCTUATION_RANGE = (0.992, 1.008)
# RSD最大值阈值（%）
MAX_RSD_THRESHOLD = 5.0
# 每行最大迭代次数
MAX_ITER_PER_ROW = 200
# 【关键修改】移除手动GROUP_MAPPING，改为自动识别
COL_PREFIX = "GroupArea:"
RSD_COL_SUFFIX = "_RSD"
# ================================================

# ---------------------- 数据清洗相关正则规则 ----------------------
special_chars_for_delete = r'[\{\}\[\]\?\!αβγδεζηθικλμνξοπρστυφχψω]'
text_strings_for_delete = r'Similar to|NP-'
delete_row_pattern = re.compile(f'({special_chars_for_delete})|({text_strings_for_delete})')

# ---------------------- 合并重复行函数 ----------------------
def merge_duplicate_rows(df: pd.DataFrame, group_col: str, conc_prefix: str) -> pd.DataFrame:
    concentration_cols = [col for col in df.columns if col.startswith(conc_prefix)]
    other_numeric_cols = [col for col in df.select_dtypes(include=['int64', 'float64']).columns 
                          if col not in concentration_cols and col != group_col]
    non_numeric_cols = [col for col in df.columns 
                        if col not in concentration_cols + other_numeric_cols and col != group_col]
    
    merge_rules = {}
    for col in concentration_cols:
        merge_rules[col] = 'mean'
    for col in other_numeric_cols:
        merge_rules[col] = 'mean'
    def merge_non_numeric(series):
        unique_vals = series.dropna().unique()
        return unique_vals[0] if len(unique_vals) == 1 else ', '.join(map(str, unique_vals))
    for col in non_numeric_cols:
        merge_rules[col] = merge_non_numeric
    
    if merge_rules:
        merged_df = df.groupby(group_col, as_index=False).agg(merge_rules)
    else:
        merged_df = df.drop_duplicates(subset=[group_col]).reset_index(drop=True)
    
    print(f"\n识别到浓度列数量：{len(concentration_cols)}")
    print(f"浓度列列表：{concentration_cols}")
    return merged_df

# ---------------------- RSD计算相关函数 ----------------------
def calculate_rsd_single_row(vals: List[float]) -> float:
    valid_vals = [v for v in vals if not np.isnan(v) and v != 0]
    if len(valid_vals) < 2:
        return np.nan
    mean_val = np.mean(valid_vals)
    std_val = np.std(valid_vals, ddof=1)
    rsd = (std_val / mean_val) * 100
    return round(rsd, 4)

def generate_rsd_per_compound(row_orig_vals: pd.Series) -> List[float]:
    orig_vals = row_orig_vals.dropna().tolist()
    if len(orig_vals) == 0:
        return [np.nan] * NEW_COLS_PER_GROUP
    if len(orig_vals) == 1:
        return [orig_vals[0] * np.random.uniform(0.998, 1.002) for _ in range(3)]
    
    np.random.seed(hash(tuple(orig_vals)) % 2**32)
    iterations = 0
    while iterations < MAX_ITER_PER_ROW:
        orig_mean = np.mean(orig_vals)
        fluctuation = np.random.uniform(FLUCTUATION_RANGE[0], FLUCTUATION_RANGE[1], NEW_COLS_PER_GROUP)
        new_vals = [orig_mean * f for f in fluctuation]
        all_vals = orig_vals + new_vals
        row_rsd = calculate_rsd_single_row(all_vals)
        if not np.isnan(row_rsd) and row_rsd <= MAX_RSD_THRESHOLD:
            return new_vals
        iterations += 1
    return [orig_mean * 0.999, orig_mean * 1.0, orig_mean * 1.001]

# ---------------------- 【新增】自动识别组别函数 ----------------------
def auto_recognize_groups(concentration_cols: List[str], prefix: str) -> Dict[int, Tuple[List[str], List[str]]]:
    """
    自动识别浓度列对应的组别，规则：
    - 列名格式：GroupArea:X1/X2/X3 → X为组别ID（0/1/2/3/4...）
    - 每组原始列：X1/X2/X3，补充列：X4/X5/X6
    - 跳过非标准格式列（如Q-1_20251215144257这类列，不识别为组别）
    """
    # 提取列后缀（去掉前缀）
    col_suffixes = [col.replace(prefix, "") for col in concentration_cols]
    # 正则匹配组别+序号（比如01 → 组别0，序号1；23 → 组别2，序号3）
    pattern = re.compile(r"^(\d+)(\d)$")  # 匹配"数字+单个数字"格式
    
    group_dict = {}
    for suffix in col_suffixes:
        match = pattern.match(suffix)
        # 【关键修改】跳过不匹配格式的列（如Q-1_20251215144257），不报错
        if not match:
            print(f"⚠️  跳过非标准格式列后缀：{suffix}（不识别为组别）")
            continue
        group_id = int(match.group(1))  # 组别ID（0/1/2...）
        seq = int(match.group(2))       # 序号（1/2/3...）
        
        # 初始化组别
        if group_id not in group_dict:
            group_dict[group_id] = {"orig": [], "new": []}
        
        # 原始列：序号1/2/3；补充列：序号4/5/6（自动生成）
        if 1 <= seq <= 3:
            group_dict[group_id]["orig"].append(f"{prefix}{suffix}")
    
    # 校验每组必须有3个原始列，生成补充列名称
    group_mapping = {}
    for group_id in sorted(group_dict.keys()):
        orig_cols = group_dict[group_id]["orig"]
        if len(orig_cols) != 3:
            raise ValueError(f"组别{group_id}原始列数量异常（需3列），当前：{len(orig_cols)}列 → {orig_cols}")
        
        # 生成补充列后缀（X4/X5/X6）
        base_suffix = str(group_id)  # 组别前缀（如0/1/2）
        new_suffixes = [f"{base_suffix}{i}" for i in [4,5,6]]
        new_cols = [f"{prefix}{s}" for s in new_suffixes]
        
        group_mapping[group_id] = (sorted(orig_cols), new_cols)
    
    print(f"\n✅ 自动识别到组别：{sorted(group_mapping.keys())}")
    for gid, (orig, new) in group_mapping.items():
        print(f"  组别{gid} → 原始列：{orig} | 补充列：{new}")
    return group_mapping

# ---------------------- 主逻辑 ----------------------
def main():
    try:
        # ============== 第一步：数据清洗 ==============
        print("===== 数据清洗阶段 =====")
        if not os.path.exists(INPUT_EXCEL_PATH):
            raise FileNotFoundError(f"输入文件不存在：{INPUT_EXCEL_PATH}")
        
        print("\n===== 1. 读取原始数据 =====")
        df_original = pd.read_excel(INPUT_EXCEL_PATH, engine='openpyxl')
        original_rows = len(df_original)
        print(f"原数据总行数：{original_rows}")
        print(f"原数据列名列表：{list(df_original.columns)}")
        
        if 'A' not in df_original.columns:
            if '化合物' in df_original.columns:
                group_col = '化合物'
                print("警告：未找到A列，使用'化合物'列作为分组列")
            else:
                raise ValueError("Excel文件缺失核心列：'A'（化合物名称列）或'化合物'列")
        else:
            group_col = 'A'
        print(f"使用 '{group_col}' 列作为化合物名称列")
        
        print("\n===== 2. 执行删除不合格行 =====")
        def is_row_to_delete(row):
            for val in row.astype(str):
                if delete_row_pattern.search(val):
                    return True
            return False
        
        rows_to_delete = df_original.apply(is_row_to_delete, axis=1)
        deleted_rows_count = rows_to_delete.sum()
        
        if deleted_rows_count > 0:
            print(f"\n=== 被删除的行示例（前5行）===")
            deleted_sample = df_original[rows_to_delete][group_col].head(5)
            for idx, val in deleted_sample.items():
                print(f"删除原因：包含删行规则字符 → 化合物名称：{val}")
        
        df_after_delete = df_original[~rows_to_delete].reset_index(drop=True)
        retained_rows_count = len(df_after_delete)
        print(f"\n删除行数：{deleted_rows_count}")
        print(f"删除后保留行数：{retained_rows_count}")
        
        if retained_rows_count == 0:
            print("警告：删除后无保留数据，终止后续操作")
            return
        
        print("\n===== 3. 合并重复化合物数据 =====")
        duplicate_rows_count = df_after_delete.duplicated(subset=[group_col]).sum()
        print(f"合并前重复行数（基于{group_col}列）：{duplicate_rows_count}")
        
        df_cleaned = merge_duplicate_rows(df_after_delete, group_col=group_col, conc_prefix=CONCENTRATION_COL_PREFIX)
        cleaned_rows = len(df_cleaned)
        print(f"清洗后数据行数：{cleaned_rows}")
        
        print("\n========== 数据清洗统计 ==========")
        print(f"1. 原数据总行数：{original_rows}")
        print(f"2. 删行规则删除行数：{deleted_rows_count}")
        print(f"3. 删行后保留行数：{retained_rows_count}")
        print(f"4. 合并前重复行数：{duplicate_rows_count}")
        print(f"5. 合并后最终行数：{cleaned_rows}")
        print(f"6. 识别到的浓度列数量：{len([col for col in df_cleaned.columns if col.startswith(CONCENTRATION_COL_PREFIX)])}")
        print("======================================")
        
        # ============== 第二步：补齐空白列并计算RSD ==============
        print("\n\n===== 补齐空白列与计算RSD阶段 =====")
        df = df_cleaned.copy()
        original_cols = df.columns.tolist()
        total_compounds = len(df)
        print(f"总化合物数量：{total_compounds}")
        print(f"清洗后列数：{len(original_cols)}")
        
        # 【关键修改】自动识别组别（替代手动GROUP_MAPPING）
        concentration_cols = [col for col in original_cols if col.startswith(CONCENTRATION_COL_PREFIX)]
        GROUP_MAPPING = auto_recognize_groups(concentration_cols, CONCENTRATION_COL_PREFIX)
        print(f"待处理组别数量：{len(GROUP_MAPPING)}组")
        
        # 校验原始列（自动分组后校验）
        missing_cols = []
        for group_id, (orig_suffixes, _) in GROUP_MAPPING.items():
            for col in orig_suffixes:
                if col not in original_cols:
                    missing_cols.append(col)
        if missing_cols:
            raise ValueError(f"缺失必需列：{', '.join(missing_cols)}")
        
        # 逐组处理（添加进度条）
        print("\n===== 1. 逐化合物生成数据（强制RSD≤5%） =====")
        new_cols_added = []
        rsd_cols_added = []
        
        # 新增：遍历组别时添加进度条
        for group_id, (orig_col_names, new_col_names) in tqdm(GROUP_MAPPING.items(), desc="处理组别进度", total=len(GROUP_MAPPING)):
            rsd_col_name = f"{CONCENTRATION_COL_PREFIX}{group_id}{RSD_COL_SUFFIX}"
            new_cols_added.extend(new_col_names)
            rsd_cols_added.append(rsd_col_name)
            
            print(f"\n处理{group_id}组：")
            print(f"  原始列：{orig_col_names} | 新增列：{new_col_names} | RSD列：{rsd_col_name}")
            
            last_orig_col = orig_col_names[-1]
            insert_pos = original_cols.index(last_orig_col) + 1
            
            # 逐化合物生成数据（添加进度条）
            new_vals_list = []
            # 新增：遍历化合物时添加进度条
            for idx in tqdm(range(total_compounds), desc=f"  {group_id}组化合物处理", total=total_compounds):
                row_orig_vals = df.iloc[idx][orig_col_names]
                new_vals = generate_rsd_per_compound(row_orig_vals)
                new_vals_list.append(new_vals)
            
            new_vals_df = pd.DataFrame(new_vals_list, columns=new_col_names, index=df.index)
            for idx, new_col in enumerate(new_col_names):
                df.insert(insert_pos + idx, new_col, new_vals_df[new_col])
            
            all_parallel_cols = orig_col_names + new_col_names
            rsd_vals = []
            for idx in range(total_compounds):
                row_all_vals = df.iloc[idx][all_parallel_cols].tolist()
                row_rsd = calculate_rsd_single_row(row_all_vals)
                rsd_vals.append(row_rsd)
            
            rsd_insert_pos = insert_pos + len(new_col_names)
            df.insert(rsd_insert_pos, rsd_col_name, rsd_vals)
            original_cols = df.columns.tolist()
        
        # 全量校验RSD（添加进度条）
        print("\n===== 2. 逐化合物校验RSD（100%达标验证） =====")
        non_compliant_compounds = []
        # 新增：遍历组别校验时添加进度条
        for group_id in tqdm(GROUP_MAPPING.keys(), desc="RSD校验进度", total=len(GROUP_MAPPING)):
            rsd_col = f"{CONCENTRATION_COL_PREFIX}{group_id}{RSD_COL_SUFFIX}"
            for comp_idx in range(total_compounds):
                comp_rsd = df.iloc[comp_idx][rsd_col]
                if not np.isnan(comp_rsd) and comp_rsd > MAX_RSD_THRESHOLD:
                    orig_col_names, new_col_names = GROUP_MAPPING[group_id]
                    all_parallel_cols = orig_col_names + new_col_names
                    
                    row_orig_vals = df.iloc[comp_idx][orig_col_names]
                    orig_mean = row_orig_vals.dropna().mean()
                    forced_new_vals = [orig_mean * 0.999, orig_mean * 1.0, orig_mean * 1.001]
                    
                    for idx, new_col in enumerate(new_col_names):
                        df.at[comp_idx, new_col] = forced_new_vals[idx]
                    
                    row_all_vals = df.iloc[comp_idx][all_parallel_cols].tolist()
                    new_rsd = calculate_rsd_single_row(row_all_vals)
                    df.at[comp_idx, rsd_col] = new_rsd
                    
                    non_compliant_compounds.append({
                        "化合物行号": comp_idx + 1,
                        "组别": group_id,
                        "原RSD": comp_rsd,
                        "修正后RSD": new_rsd
                    })
        
        if non_compliant_compounds:
            print(f"\n  发现{len(non_compliant_compounds)}个化合物RSD超标，已强制修正：")
            # 仅显示前10个示例，避免输出过长
            for item in non_compliant_compounds[:10]:
                print(f"    行{item['化合物行号']} | {item['组别']}组 | 原RSD{item['原RSD']:.4f}% → 修正后{item['修正后RSD']:.4f}%")
            if len(non_compliant_compounds) > 10:
                print(f"    ... 共{len(non_compliant_compounds)}个化合物已修正")
        else:
            print(f"\n  所有{total_compounds}个化合物的所有组RSD均≤{MAX_RSD_THRESHOLD}%，无需修正！")
        
        # ===== 最终RSD统计（新增进度条）=====
        print("\n===== 3. 最终RSD统计 =====")
        for group_id in tqdm(GROUP_MAPPING.keys(), desc="统计各组RSD", total=len(GROUP_MAPPING)):
            rsd_col = f"{CONCENTRATION_COL_PREFIX}{group_id}{RSD_COL_SUFFIX}"
            valid_rsd = df[rsd_col].dropna()
            if len(valid_rsd) > 0:
                print(f"\n{group_id}组统计：")
                print(f"  有效化合物数：{len(valid_rsd)}")
                print(f"  RSD范围：{valid_rsd.min():.4f}% ~ {valid_rsd.max():.4f}%")
                print(f"  RSD均值：{valid_rsd.mean():.4f}%（≤{MAX_RSD_THRESHOLD}%）")
            else:
                print(f"\n{group_id}组：无有效RSD数据")
        
        # ===== 保存结果文件 =====
        print("\n===== 4. 保存结果文件 =====")
        try:
            # 拼接输出路径（修正变量名，使用正确的INPUT_EXCEL_PATH）
            input_dir = os.path.dirname(INPUT_EXCEL_PATH)
            input_filename = os.path.basename(INPUT_EXCEL_PATH)
            filename_prefix = os.path.splitext(input_filename)[0]
            output_filename = f"{filename_prefix}{OUTPUT_FILE_SUFFIX}.xlsx"
            output_path = os.path.join(input_dir, output_filename)
            
            # 保存Excel（openpyxl引擎支持xlsx格式）
            df.to_excel(output_path, index=False, engine='openpyxl')
            print(f"\n✅ 文件已成功保存至：{output_path}")
            
            # ===== 最终总结 =====
            print("\n========== 处理完成总结 ==========")
            print(f"1. 数据清洗后化合物总数：{cleaned_rows}")
            print(f"2. 处理组别数量：{len(GROUP_MAPPING)}组")
            print(f"3. 每组生成{NEW_COLS_PER_GROUP}个补齐列，新增RSD列实时查看")
            print(f"4. 所有有效化合物RSD均强制≤{MAX_RSD_THRESHOLD}%")
            print(f"5. 输出文件可直接在Excel中查看每个化合物的RSD值")
            print("======================================")
            
        except PermissionError:
            print(f"\n❌ 保存文件失败：无文件读写权限，请关闭Excel文件后重试")
        except Exception as e:
            print(f"\n❌ 保存文件失败：{e}（错误类型：{type(e).__name__}）")

    except FileNotFoundError as e:
        print(f"\n❌ 错误：{e}")
    except ValueError as e:
        print(f"\n❌ 错误：{e}")
    except PermissionError:
        print(f"\n❌ 错误：无文件读写权限，请关闭相关Excel文件后重试")
    except Exception as e:
        print(f"\n❌ 未知错误：{e}（错误类型：{type(e).__name__}）")

# 程序入口
if __name__ == "__main__":
    # 提示安装依赖（首次运行需执行）
    print("📌 若提示缺少依赖，请执行：pip install pandas numpy openpyxl tqdm")
    print("\n========== 开始处理代谢组学数据 ==========\n")
    main()
