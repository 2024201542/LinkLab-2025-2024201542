#include "fle.hpp"
#include <iomanip>
#include <iostream>
#include <algorithm>

void FLE_nm(const FLEObject& obj)
{
    // 创建可修改的符号表副本用于排序
    std::vector<Symbol> symbols = obj.symbols;
    
    // 按地址排序（先按节名，再按节内偏移）
    std::sort(symbols.begin(), symbols.end(), 
        [](const Symbol& a, const Symbol& b) {
            // 未定义符号放在最后
            if (a.type == SymbolType::UNDEFINED && b.type != SymbolType::UNDEFINED) return false;
            if (a.type != SymbolType::UNDEFINED && b.type == SymbolType::UNDEFINED) return true;
            
            // 按节名比较
            if (a.section != b.section) return a.section < b.section;
            
            // 同节内按偏移排序
            return a.offset < b.offset;
        });
    
    // 遍历并输出每个符号
    for (const auto& sym : symbols) {
        // 地址：16位十六进制，未定义符号为0
        uint64_t address = 0;
        if (sym.type != SymbolType::UNDEFINED) {
            // 注意：这里输出的是节内偏移，不是最终地址
            address = sym.offset;
        }
        
        // 类型字符
        char type_char = '?';
        
        if (sym.type == SymbolType::UNDEFINED) {
            type_char = 'U';
        } else {
            // 根据节名和符号类型决定类型字符
            std::string sec = sym.section;
            
            // 判断节类型
            bool is_text = (sec.find(".text") == 0);
            bool is_data = (sec.find(".data") == 0);
            bool is_bss = (sec.find(".bss") == 0);
            bool is_rodata = (sec.find(".rodata") == 0);
            
            // 决定基础类型字符
            char base_char = '?';
            if (is_text) base_char = 'T';
            else if (is_data) base_char = 'D';
            else if (is_bss) base_char = 'B';
            else if (is_rodata) base_char = 'R';
            
            // 根据符号类型调整大小写
            switch (sym.type) {
                case SymbolType::LOCAL:
                    type_char = std::tolower(base_char);
                    break;
                case SymbolType::GLOBAL:
                    type_char = base_char;
                    break;
                case SymbolType::WEAK:
                    // 弱符号：用对应的大写字母，但如果是代码用'W'，数据用'V'
                    if (is_text) type_char = 'W';
                    else if (is_data || is_bss || is_rodata) type_char = 'V';
                    else type_char = base_char;
                    break;
                default:
                    type_char = base_char;
            }
        }
        
        // 输出
        std::cout << std::hex << std::setfill('0') << std::setw(16) 
                  << address << " " << type_char << " " << sym.name << std::endl;
    }
}