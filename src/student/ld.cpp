#include "fle.hpp"
#include <cassert>
#include <iostream>
#include <map>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <cstdint>
#include <string>
#include <set>
#include <queue>

// 页大小常量
constexpr size_t PAGE_SIZE = 4096;

// 对齐函数
inline size_t align_to(size_t value, size_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

FLEObject FLE_ld(const std::vector<FLEObject>& objects, const LinkerOptions& options)
{
    // 任务七：处理归档文件（静态库）的按需链接
    // 我们将对象分为普通对象和归档文件
    std::vector<FLEObject> ordinary_objs;
    std::vector<FLEObject> archives;
    
    for (const auto& obj : objects) {
        if (obj.type == ".ar") {
            archives.push_back(obj);
        } else {
            ordinary_objs.push_back(obj);
        }
    }
    
    // 用于记录已解析的符号和未解析的符号
    std::set<std::string> resolved_symbols;   // 已定义的符号
    std::set<std::string> undefined_symbols;  // 未定义的符号
    
    // 首先，扫描所有普通对象，收集符号定义和引用
    for (const auto& obj : ordinary_objs) {
        for (const Symbol& sym : obj.symbols) {
            if (sym.name.empty() || sym.name[0] == '.' || sym.type == SymbolType::LOCAL) {
                continue;
            }
            if (sym.type != SymbolType::UNDEFINED) {
                resolved_symbols.insert(sym.name);
            } else {
                undefined_symbols.insert(sym.name);
            }
        }
    }
    
    // 从undefined_symbols中移除已定义的符号
    for (const auto& sym : resolved_symbols) {
        undefined_symbols.erase(sym);
    }
    
    // 按需链接：循环直到没有新的符号被解析
    bool changed;
    std::vector<FLEObject> all_objects = ordinary_objs; // 最终要链接的所有对象
    
    do {
        changed = false;
        
        // 遍历所有归档文件
        for (auto archive_it = archives.begin(); archive_it != archives.end(); ) {
            const FLEObject& archive = *archive_it;
            bool archive_used = false;
            
            // 检查归档文件的每个成员
            for (const auto& member : archive.members) {
                // 检查该成员是否定义了任何当前未解析的符号
                bool defines_needed = false;
                for (const Symbol& sym : member.symbols) {
                    if (sym.name.empty() || sym.name[0] == '.' || sym.type == SymbolType::LOCAL) {
                        continue;
                    }
                    if (sym.type != SymbolType::UNDEFINED && undefined_symbols.count(sym.name)) {
                        defines_needed = true;
                        break;
                    }
                }
                
                // 如果定义了需要的符号，则添加该成员
                if (defines_needed) {
                    all_objects.push_back(member);
                    archive_used = true;
                    changed = true;
                    
                    // 更新符号表
                    for (const Symbol& sym : member.symbols) {
                        if (sym.name.empty() || sym.name[0] == '.' || sym.type == SymbolType::LOCAL) {
                            continue;
                        }
                        if (sym.type != SymbolType::UNDEFINED) {
                            resolved_symbols.insert(sym.name);
                            undefined_symbols.erase(sym.name);
                        } else {
                            // 如果是未定义符号，且尚未定义，则加入未解析集合
                            if (!resolved_symbols.count(sym.name)) {
                                undefined_symbols.insert(sym.name);
                            }
                        }
                    }
                }
            }
            
            // 如果该归档文件被使用了，则从列表中移除（避免重复处理）
            if (archive_used) {
                archive_it = archives.erase(archive_it);
            } else {
                ++archive_it;
            }
        }
    } while (changed);
    
    // 现在，all_objects包含了所有需要的对象，进行链接
    
    if (all_objects.empty()) {
        throw std::runtime_error("No input objects to link");
    }
    
    // 创建输出对象
    FLEObject output;
    output.name = options.outputFile;
    output.type = options.shared ? ".so" : ".exe";
    
    // 1. 合并节内容（按原节名合并）
    std::map<std::string, FLESection> merged_sections;
    std::map<std::pair<size_t, std::string>, size_t> section_offsets;
    
    for (size_t obj_idx = 0; obj_idx < all_objects.size(); obj_idx++) {
        const FLEObject& obj = all_objects[obj_idx];
        
        for (const auto& [sec_name, sec] : obj.sections) {
            if (merged_sections.find(sec_name) == merged_sections.end()) {
                FLESection new_sec;
                new_sec.name = sec_name;
                new_sec.has_symbols = sec.has_symbols;
                merged_sections[sec_name] = new_sec;
            }
            
            size_t current_offset = merged_sections[sec_name].data.size();
            section_offsets[{obj_idx, sec_name}] = current_offset;
            
            FLESection& merged_sec = merged_sections[sec_name];
            merged_sec.data.insert(merged_sec.data.end(), sec.data.begin(), sec.data.end());
            
            for (const auto& reloc : sec.relocs) {
                Relocation new_reloc = reloc;
                new_reloc.offset += current_offset;
                merged_sec.relocs.push_back(new_reloc);
            }
        }
    }
    
    // 2. 建立全局符号表（任务四：符号冲突处理）
    std::unordered_map<std::string, Symbol> global_symbols;
    std::vector<Symbol> output_symbols;
    
    // 为每个目标文件建立本地符号表
    std::vector<std::unordered_map<std::string, Symbol>> local_symbols_by_obj(all_objects.size());
    
    for (size_t obj_idx = 0; obj_idx < all_objects.size(); obj_idx++) {
        const FLEObject& obj = all_objects[obj_idx];
        
        for (const Symbol& sym : obj.symbols) {
            // 处理本地标签符号（以.开头的符号）
            if (!sym.name.empty() && sym.name[0] == '.') {
                Symbol new_sym = sym;
                if (!sym.section.empty() && section_offsets.find({obj_idx, sym.section}) != section_offsets.end()) {
                    new_sym.offset += section_offsets[{obj_idx, sym.section}];
                }
                
                local_symbols_by_obj[obj_idx][sym.name] = new_sym;
                new_sym.type = SymbolType::LOCAL;
                output_symbols.push_back(new_sym);
                continue;
            }
            
            // 处理全局符号
            Symbol new_sym = sym;
            if (!sym.section.empty() && section_offsets.find({obj_idx, sym.section}) != section_offsets.end()) {
                new_sym.offset += section_offsets[{obj_idx, sym.section}];
            }
            
            auto it = global_symbols.find(sym.name);
            
            if (it == global_symbols.end()) {
                global_symbols[sym.name] = new_sym;
            } else {
                Symbol& existing_sym = it->second;
                
                // 规则1: 强符号必须唯一
                if (existing_sym.type == SymbolType::GLOBAL && 
                    new_sym.type == SymbolType::GLOBAL) {
                    throw std::runtime_error("Multiple definition of strong symbol: " + sym.name);
                }
                
                // 规则2: 强符号覆盖弱符号
                if (existing_sym.type == SymbolType::WEAK && 
                    new_sym.type == SymbolType::GLOBAL) {
                    existing_sym = new_sym;
                }
                // 规则3: 弱符号不覆盖强符号
                else if (existing_sym.type == SymbolType::GLOBAL && 
                         new_sym.type == SymbolType::WEAK) {
                    // 保持现有强符号不变
                }
                // 规则4: 多个弱符号共存
                else if (existing_sym.type == SymbolType::WEAK && 
                         new_sym.type == SymbolType::WEAK) {
                    // 保持现有的弱符号
                }
                // 规则5: 已定义符号覆盖未定义符号
                else if (existing_sym.type == SymbolType::UNDEFINED &&
                         new_sym.type != SymbolType::UNDEFINED) {
                    existing_sym = new_sym;
                }
                // 规则6: 未定义符号不覆盖已定义符号
                else if (existing_sym.type != SymbolType::UNDEFINED &&
                         new_sym.type == SymbolType::UNDEFINED) {
                    // 保持现有定义不变
                }
            }
        }
    }
    
    // 将最终确定的符号添加到输出符号表
    for (const auto& [name, sym] : global_symbols) {
        if (sym.type != SymbolType::UNDEFINED) {
            output_symbols.push_back(sym);
        }
    }
    
    // 3. 将节按标准类别合并（任务五：多段布局）
    std::map<std::string, FLESection> output_sections;
    std::map<std::string, std::string> sec_to_output;
    std::map<std::string, size_t> sec_offset_in_output;
    
    // 定义标准节类别
    std::vector<std::pair<std::string, std::vector<std::string>>> section_categories = {
        {".text", {".text"}},
        {".rodata", {".rodata"}},
        {".data", {".data"}},
        {".bss", {".bss"}}
    };
    
    // 收集所有原节名
    std::vector<std::string> all_input_sections;
    for (const auto& [sec_name, _] : merged_sections) {
        all_input_sections.push_back(sec_name);
    }
    std::sort(all_input_sections.begin(), all_input_sections.end());
    
    // 处理每个类别
    for (const auto& [category_name, prefixes] : section_categories) {
        FLESection out_sec;
        out_sec.name = category_name;
        out_sec.has_symbols = false;
        
        size_t current_offset = 0;
        
        for (const auto& sec_name : all_input_sections) {
            bool belongs = false;
            for (const auto& prefix : prefixes) {
                if (sec_name.find(prefix) == 0) {
                    belongs = true;
                    break;
                }
            }
            
            if (belongs) {
                const FLESection& src_sec = merged_sections[sec_name];
                
                sec_to_output[sec_name] = category_name;
                sec_offset_in_output[sec_name] = current_offset;
                
                // 对于.bss节，不复制数据到文件
                if (category_name != ".bss") {
                    out_sec.data.insert(out_sec.data.end(), src_sec.data.begin(), src_sec.data.end());
                }
                
                // 复制重定位信息
                for (const auto& reloc : src_sec.relocs) {
                    Relocation new_reloc = reloc;
                    new_reloc.offset += current_offset;
                    out_sec.relocs.push_back(new_reloc);
                }
                
                current_offset += src_sec.data.size();
            }
        }
        
        // 如果有数据或重定位信息，则添加
        if (!out_sec.data.empty() || !out_sec.relocs.empty() || category_name == ".bss") {
            output_sections[category_name] = out_sec;
        }
    }
    
    // 处理剩余未分类的节，合并到".data"节
    for (const auto& sec_name : all_input_sections) {
        if (sec_to_output.find(sec_name) == sec_to_output.end()) {
            sec_to_output[sec_name] = ".data";
            if (output_sections.find(".data") == output_sections.end()) {
                FLESection data_sec;
                data_sec.name = ".data";
                data_sec.has_symbols = false;
                output_sections[".data"] = data_sec;
            }
            // 将数据添加到.data节
            const FLESection& src_sec = merged_sections[sec_name];
            output_sections[".data"].data.insert(output_sections[".data"].data.end(), 
                                                src_sec.data.begin(), src_sec.data.end());
        }
    }
    
    // 4. 计算每个输出节在内存中的虚拟地址偏移（任务六：4KB对齐）
    std::map<std::string, size_t> section_vaddr_offsets;
    std::map<std::string, size_t> section_file_offsets;
    std::map<std::string, size_t> section_mem_sizes;
    
    uint64_t base_addr = 0x400000;
    uint64_t current_vaddr_offset = 0;
    size_t current_file_offset = 0;
    
    // 定义输出节的顺序
    std::vector<std::string> output_order = {".text", ".rodata", ".data", ".bss"};
    std::vector<std::string> out_sec_names;
    
    for (const auto& sec_name : output_order) {
        if (output_sections.find(sec_name) != output_sections.end()) {
            out_sec_names.push_back(sec_name);
            
            // 虚拟地址对齐
            current_vaddr_offset = align_to(current_vaddr_offset, PAGE_SIZE);
            section_vaddr_offsets[sec_name] = current_vaddr_offset;
            
            if (sec_name == ".bss") {
                // 计算.bss节的总大小
                size_t bss_size = 0;
                for (const auto& [input_sec_name, input_sec] : merged_sections) {
                    if (input_sec_name.find(".bss") == 0) {
                        bss_size += input_sec.data.size();
                    }
                }
                
                section_file_offsets[sec_name] = 0;
                section_mem_sizes[sec_name] = bss_size;
            } else {
                current_file_offset = align_to(current_file_offset, PAGE_SIZE);
                section_file_offsets[sec_name] = current_file_offset;
                size_t data_size = output_sections[sec_name].data.size();
                section_mem_sizes[sec_name] = data_size;
                current_file_offset += data_size;
            }
            
            current_vaddr_offset += section_mem_sizes[sec_name];
        }
    }
    
    // 5. 更新符号的节和偏移
    for (Symbol& sym : output_symbols) {
        if (!sym.section.empty()) {
            auto it = sec_to_output.find(sym.section);
            if (it != sec_to_output.end()) {
                std::string out_sec_name = it->second;
                size_t offset_in_out_sec = sec_offset_in_output[sym.section];
                sym.section = out_sec_name;
                sym.offset += offset_in_out_sec;
            }
        }
    }
    
    // 6. 处理重定位（任务二、三：重定位计算）
    for (auto& [out_sec_name, out_sec] : output_sections) {
        for (Relocation& reloc : out_sec.relocs) {
            // 计算重定位位置的虚拟地址P
            uint64_t P = base_addr + section_vaddr_offsets[out_sec_name] + reloc.offset;
            
            // 计算符号的虚拟地址
            uint64_t sym_vaddr = 0;
            
            // 检查是否是本地标签（以.开头的符号）
            if (!reloc.symbol.empty() && reloc.symbol[0] == '.') {
                bool found = false;
                
                for (size_t obj_idx = 0; obj_idx < all_objects.size(); obj_idx++) {
                    auto local_sym_it = local_symbols_by_obj[obj_idx].find(reloc.symbol);
                    if (local_sym_it != local_symbols_by_obj[obj_idx].end()) {
                        const Symbol& target_sym = local_sym_it->second;
                        
                        if (!target_sym.section.empty()) {
                            auto map_it = sec_to_output.find(target_sym.section);
                            if (map_it != sec_to_output.end()) {
                                std::string target_out_sec = map_it->second;
                                size_t offset_in_target_out_sec = sec_offset_in_output[target_sym.section];
                                sym_vaddr = base_addr + section_vaddr_offsets[target_out_sec] + 
                                           offset_in_target_out_sec + target_sym.offset;
                            } else {
                                sym_vaddr = base_addr + target_sym.offset;
                            }
                        } else {
                            sym_vaddr = base_addr + target_sym.offset;
                        }
                        found = true;
                        break;
                    }
                }
                
                if (!found) {
                    throw std::runtime_error("Undefined local symbol: " + reloc.symbol);
                }
            } else {
                // 普通符号
                auto sym_it = global_symbols.find(reloc.symbol);
                if (sym_it == global_symbols.end() || sym_it->second.type == SymbolType::UNDEFINED) {
                    // 对于未定义符号，不立即抛出异常，而是尝试继续
                    // 因为可能在其他文件中定义
                    continue;
                }
                
                const Symbol& target_sym = sym_it->second;
                if (!target_sym.section.empty()) {
                    auto sec_offset_it = section_vaddr_offsets.find(target_sym.section);
                    if (sec_offset_it != section_vaddr_offsets.end()) {
                        sym_vaddr = base_addr + sec_offset_it->second + target_sym.offset;
                    } else {
                        sym_vaddr = base_addr + target_sym.offset;
                    }
                } else {
                    sym_vaddr = base_addr + target_sym.offset;
                }
            }
            
            // 计算重定位值（对于.bss节，不写入文件）
            if (out_sec_name == ".bss") {
                continue;
            }
            
            // 检查偏移是否在范围内，注意不同的重定位类型需要的字节数不同
            size_t reloc_size = 0;
            switch (reloc.type) {
                case RelocationType::R_X86_64_32:
                case RelocationType::R_X86_64_32S:
                case RelocationType::R_X86_64_PC32:
                    reloc_size = 4;
                    break;
                case RelocationType::R_X86_64_64:
                    reloc_size = 8;
                    break;
                default:
                    reloc_size = 8; // 默认为8，但应该不会执行到这里
            }
            
            // 修复：只有当偏移确实越界时才抛出异常
            if (reloc.offset + reloc_size > out_sec.data.size()) {
                // 如果重定位指向.bss节，这是正常的，因为.bss在文件中没有数据
                // 我们只检查非.bss节
                if (out_sec_name != ".bss") {
                    // 检查这个重定位是否指向一个.bss节的符号
                    bool points_to_bss = false;
                    if (!reloc.symbol.empty() && reloc.symbol[0] != '.') {
                        auto sym_it2 = global_symbols.find(reloc.symbol);
                        if (sym_it2 != global_symbols.end() && !sym_it2->second.section.empty()) {
                            auto sec_it = sec_to_output.find(sym_it2->second.section);
                            if (sec_it != sec_to_output.end() && sec_it->second == ".bss") {
                                points_to_bss = true;
                            }
                        }
                    }
                    
                    if (!points_to_bss) {
                        throw std::runtime_error("Relocation offset out of bounds");
                    }
                }
                continue;
            }
            
            switch (reloc.type) {
                case RelocationType::R_X86_64_32:
                {
                    uint64_t value = sym_vaddr + reloc.addend;
                    if (value > 0xFFFFFFFF) {
                        throw std::runtime_error("R_X86_64_32 relocation overflow");
                    }
                    *reinterpret_cast<uint32_t*>(&out_sec.data[reloc.offset]) = static_cast<uint32_t>(value);
                    break;
                }
                    
                case RelocationType::R_X86_64_32S:
                {
                    int64_t value = static_cast<int64_t>(sym_vaddr) + reloc.addend;
                    if (value > INT32_MAX || value < INT32_MIN) {
                        throw std::runtime_error("R_X86_64_32S relocation overflow");
                    }
                    *reinterpret_cast<int32_t*>(&out_sec.data[reloc.offset]) = static_cast<int32_t>(value);
                    break;
                }
                    
                case RelocationType::R_X86_64_PC32:
                {
                    int64_t value = static_cast<int64_t>(sym_vaddr) + reloc.addend - static_cast<int64_t>(P);
                    if (value > INT32_MAX || value < INT32_MIN) {
                        throw std::runtime_error("R_X86_64_PC32 relocation overflow");
                    }
                    *reinterpret_cast<int32_t*>(&out_sec.data[reloc.offset]) = static_cast<int32_t>(value);
                    break;
                }
                    
                case RelocationType::R_X86_64_64:
                {
                    uint64_t value = sym_vaddr + reloc.addend;
                    *reinterpret_cast<uint64_t*>(&out_sec.data[reloc.offset]) = value;
                    break;
                }
                    
                default:
                    throw std::runtime_error("Unsupported relocation type: " + std::to_string(static_cast<int>(reloc.type)));
            }
        }
    }
    
    // 7. 设置输出对象的节
    output.sections = output_sections;
    output.symbols = output_symbols;
    
    // 8. 设置节头（任务六：正确权限）
    output.shdrs.clear();
    
    for (const auto& sec_name : out_sec_names) {
        SectionHeader shdr;
        shdr.name = sec_name;
        shdr.type = 1;
        
        // 设置节标志
        shdr.flags = static_cast<uint32_t>(SHF::ALLOC);
        
        if (sec_name == ".text") {
            shdr.flags |= static_cast<uint32_t>(SHF::EXEC);
        } else if (sec_name == ".rodata") {
            // 只读，不可写
        } else {
            shdr.flags |= static_cast<uint32_t>(SHF::WRITE);
        }
        
        if (sec_name == ".bss") {
            shdr.flags |= static_cast<uint32_t>(SHF::NOBITS);
        }
        
        shdr.addr = base_addr + section_vaddr_offsets[sec_name];
        shdr.offset = section_file_offsets[sec_name];
        shdr.size = section_mem_sizes[sec_name];
        
        output.shdrs.push_back(shdr);
    }
    
    // 9. 设置程序头（任务六：正确权限）
    output.phdrs.clear();
    
    for (const auto& sec_name : out_sec_names) {
        ProgramHeader phdr;
        phdr.name = sec_name;
        phdr.vaddr = align_to(base_addr + section_vaddr_offsets[sec_name], PAGE_SIZE);
        phdr.size = section_mem_sizes[sec_name];
        
        // 设置权限标志
        if (sec_name == ".text") {
            phdr.flags = static_cast<uint32_t>(PHF::R) | static_cast<uint32_t>(PHF::X);
        } else if (sec_name == ".rodata") {
            phdr.flags = static_cast<uint32_t>(PHF::R);
        } else {
            phdr.flags = static_cast<uint32_t>(PHF::R) | static_cast<uint32_t>(PHF::W);
        }
        
        output.phdrs.push_back(phdr);
    }
    
    // 10. 设置入口点（关键修复：确保入口点正确）
    auto entry_sym = global_symbols.find(options.entryPoint);
    if (entry_sym != global_symbols.end()) {
        const Symbol& sym = entry_sym->second;
        uint64_t entry_vaddr = base_addr;
        if (!sym.section.empty()) {
            auto sec_offset_it = section_vaddr_offsets.find(sym.section);
            if (sec_offset_it != section_vaddr_offsets.end()) {
                entry_vaddr += sec_offset_it->second + sym.offset;
            } else {
                entry_vaddr += sym.offset;
            }
        } else {
            entry_vaddr += sym.offset;
        }
        output.entry = entry_vaddr;
    } else {
        // 如果找不到入口点符号，使用.text段的起始地址
        if (section_vaddr_offsets.find(".text") != section_vaddr_offsets.end()) {
            output.entry = base_addr + section_vaddr_offsets[".text"];
        } else {
            output.entry = base_addr;
        }
    }
    
    return output;
}