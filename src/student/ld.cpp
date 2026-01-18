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

// 页大小常量
constexpr size_t PAGE_SIZE = 4096;

// 对齐函数
inline size_t align_to(size_t value, size_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

// 辅助函数：根据前缀匹配获取输出节名
static std::string get_output_section_name(const std::string& sec_name) {
    if (sec_name.find(".text") == 0) return ".text";
    if (sec_name.find(".rodata") == 0) return ".rodata";
    if (sec_name.find(".data") == 0) return ".data";
    if (sec_name.find(".bss") == 0) return ".bss";
    return ".data";  // 默认放到 .data
}

FLEObject FLE_ld(const std::vector<FLEObject>& objects, const LinkerOptions& options)
{
    // 任务七：处理归档文件（静态库）的按需链接
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
    std::set<std::string> resolved_symbols;
    std::set<std::string> undefined_symbols;
    
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
    std::vector<FLEObject> all_objects = ordinary_objs;
    
    do {
        changed = false;
        
        for (auto archive_it = archives.begin(); archive_it != archives.end(); ) {
            const FLEObject& archive = *archive_it;
            bool archive_used = false;
            
            for (const auto& member : archive.members) {
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
                
                if (defines_needed) {
                    all_objects.push_back(member);
                    archive_used = true;
                    changed = true;
                    
                    for (const Symbol& sym : member.symbols) {
                        if (sym.name.empty() || sym.name[0] == '.' || sym.type == SymbolType::LOCAL) {
                            continue;
                        }
                        if (sym.type != SymbolType::UNDEFINED) {
                            resolved_symbols.insert(sym.name);
                            undefined_symbols.erase(sym.name);
                        } else {
                            if (!resolved_symbols.count(sym.name)) {
                                undefined_symbols.insert(sym.name);
                            }
                        }
                    }
                }
            }
            
            if (archive_used) {
                archive_it = archives.erase(archive_it);
            } else {
                ++archive_it;
            }
        }
    } while (changed);
    
    if (all_objects.empty()) {
        throw std::runtime_error("No input objects to link");
    }
    
    // 创建输出对象
    FLEObject output;
    output.name = options.outputFile;
    output.type = options.shared ? ".so" : ".exe";
    
    // 1. 合并节内容
    std::map<std::string, FLESection> merged_sections;
    std::map<std::pair<size_t, std::string>, size_t> section_offsets;
    std::map<std::pair<size_t, std::string>, size_t> section_sizes;
    
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
            section_sizes[{obj_idx, sec_name}] = sec.data.size();
            
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
                
                if (existing_sym.type == SymbolType::GLOBAL && 
                    new_sym.type == SymbolType::GLOBAL) {
                    throw std::runtime_error("Multiple definition of strong symbol: " + sym.name);
                }
                
                if (existing_sym.type == SymbolType::WEAK && 
                    new_sym.type == SymbolType::GLOBAL) {
                    existing_sym = new_sym;
                }
                else if (existing_sym.type == SymbolType::GLOBAL && 
                         new_sym.type == SymbolType::WEAK) {
                    // 保持现有强符号不变
                }
                else if (existing_sym.type == SymbolType::WEAK && 
                         new_sym.type == SymbolType::WEAK) {
                    // 保持现有的弱符号
                }
                else if (existing_sym.type == SymbolType::UNDEFINED &&
                         new_sym.type != SymbolType::UNDEFINED) {
                    existing_sym = new_sym;
                }
                else if (existing_sym.type != SymbolType::UNDEFINED &&
                         new_sym.type == SymbolType::UNDEFINED) {
                    // 保持现有定义不变
                }
            }
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
                
                if (category_name != ".bss") {
                    out_sec.data.insert(out_sec.data.end(), src_sec.data.begin(), src_sec.data.end());
                }
                
                for (const auto& reloc : src_sec.relocs) {
                    Relocation new_reloc = reloc;
                    new_reloc.offset += current_offset;
                    out_sec.relocs.push_back(new_reloc);
                }
                
                current_offset += src_sec.data.size();
            }
        }
        
        if (!out_sec.data.empty() || !out_sec.relocs.empty() || category_name == ".bss") {
            output_sections[category_name] = out_sec;
        }
    }
    
    // 处理剩余未分类的节
    for (const auto& sec_name : all_input_sections) {
        if (sec_to_output.find(sec_name) == sec_to_output.end()) {
            sec_to_output[sec_name] = ".data";
            if (output_sections.find(".data") == output_sections.end()) {
                FLESection data_sec;
                data_sec.name = ".data";
                data_sec.has_symbols = false;
                output_sections[".data"] = data_sec;
            }
            
            size_t current_offset = output_sections[".data"].data.size();
            sec_offset_in_output[sec_name] = current_offset;
            
            const FLESection& src_sec = merged_sections[sec_name];
            output_sections[".data"].data.insert(output_sections[".data"].data.end(), 
                                                src_sec.data.begin(), src_sec.data.end());
            
            for (const auto& reloc : src_sec.relocs) {
                Relocation new_reloc = reloc;
                new_reloc.offset += current_offset;
                output_sections[".data"].relocs.push_back(new_reloc);
            }
        }
    }
    
    // 4. 计算每个输出节在内存中的虚拟地址偏移（任务六：4KB对齐）
    std::map<std::string, size_t> section_vaddr_offsets;
    std::map<std::string, size_t> section_file_offsets;
    std::map<std::string, size_t> section_mem_sizes;
    
    uint64_t base_addr = 0x400000;
    uint64_t current_vaddr_offset = 0;
    size_t current_file_offset = 0;
    
    std::vector<std::string> output_order = {".text", ".rodata", ".data", ".bss"};
    std::vector<std::string> out_sec_names;
    
    for (const auto& sec_name : output_order) {
        if (output_sections.find(sec_name) != output_sections.end()) {
            out_sec_names.push_back(sec_name);
            
            current_vaddr_offset = align_to(current_vaddr_offset, PAGE_SIZE);
            section_vaddr_offsets[sec_name] = current_vaddr_offset;
            
            if (sec_name == ".bss") {
                size_t bss_size = 0;
                for (const auto& [input_sec_name, input_sec] : merged_sections) {
                    if (input_sec_name.find(".bss") == 0) {
                        bss_size += input_sec.data.size();
                    }
                }
                
                section_file_offsets[sec_name] = 0;
                section_mem_sizes[sec_name] = bss_size;
            } else {
                section_file_offsets[sec_name] = current_file_offset;
                size_t data_size = output_sections[sec_name].data.size();
                section_mem_sizes[sec_name] = data_size;
                current_file_offset += data_size;
            }
            
            current_vaddr_offset += section_mem_sizes[sec_name];
        }
    }
    
    // 5. 创建 merged section 到虚拟地址偏移的映射
    // 这是关键！每个 merged section 在最终虚拟地址空间中的起始偏移
    std::map<std::string, size_t> merged_sec_vaddr;
    for (const auto& [sec_name, _] : merged_sections) {
        std::string out_sec = get_output_section_name(sec_name);
        if (section_vaddr_offsets.find(out_sec) != section_vaddr_offsets.end()) {
            auto offset_it = sec_offset_in_output.find(sec_name);
            if (offset_it != sec_offset_in_output.end()) {
                merged_sec_vaddr[sec_name] = section_vaddr_offsets[out_sec] + offset_it->second;
            } else {
                merged_sec_vaddr[sec_name] = section_vaddr_offsets[out_sec];
            }
        }
    }
    
    // 6. 更新符号的节和偏移
    // 同时更新 global_symbols 和 output_symbols
    for (auto& [name, sym] : global_symbols) {
        if (!sym.section.empty()) {
            auto it = sec_to_output.find(sym.section);
            if (it != sec_to_output.end()) {
                std::string out_sec_name = it->second;
                auto offset_it = sec_offset_in_output.find(sym.section);
                if (offset_it != sec_offset_in_output.end()) {
                    sym.offset += offset_it->second;
                }
                sym.section = out_sec_name;
            } else {
                // 使用前缀匹配
                sym.section = get_output_section_name(sym.section);
            }
        }
    }
    
    // 将最终确定的符号添加到输出符号表
    for (const auto& [name, sym] : global_symbols) {
        if (sym.type != SymbolType::UNDEFINED) {
            output_symbols.push_back(sym);
        }
    }
    
    // 更新 output_symbols 中的本地符号
    for (Symbol& sym : output_symbols) {
        if (sym.type == SymbolType::LOCAL && !sym.section.empty()) {
            auto it = sec_to_output.find(sym.section);
            if (it != sec_to_output.end()) {
                std::string out_sec_name = it->second;
                auto offset_it = sec_offset_in_output.find(sym.section);
                if (offset_it != sec_offset_in_output.end()) {
                    sym.offset += offset_it->second;
                }
                sym.section = out_sec_name;
            } else {
                sym.section = get_output_section_name(sym.section);
            }
        }
    }
    
    // 7. 处理重定位（任务二、三：重定位计算）
    for (auto& [out_sec_name, out_sec] : output_sections) {
        for (Relocation& reloc : out_sec.relocs) {
            uint64_t P = base_addr + section_vaddr_offsets[out_sec_name] + reloc.offset;
            uint64_t sym_vaddr = 0;
            
            // 检查是否是本地标签（以.开头的符号）
            if (!reloc.symbol.empty() && reloc.symbol[0] == '.') {
                bool found = false;
                
                // 找到这个重定位原本在哪个merged section中
                std::string orig_merged_sec;
                size_t reloc_offset_in_merged = reloc.offset;
                
                for (const auto& [sec_name, _] : merged_sections) {
                    auto sec_to_out_it = sec_to_output.find(sec_name);
                    if (sec_to_out_it != sec_to_output.end() && sec_to_out_it->second == out_sec_name) {
                        auto sec_offset_it = sec_offset_in_output.find(sec_name);
                        if (sec_offset_it != sec_offset_in_output.end()) {
                            size_t sec_offset = sec_offset_it->second;
                            size_t sec_size = merged_sections[sec_name].data.size();
                            if (reloc.offset >= sec_offset && reloc.offset < sec_offset + sec_size) {
                                orig_merged_sec = sec_name;
                                reloc_offset_in_merged = reloc.offset - sec_offset;
                                break;
                            }
                        }
                    }
                }
                
                if (orig_merged_sec.empty()) {
                    throw std::runtime_error("Cannot find original section for relocation");
                }
                
                // 找到这个重定位来自哪个目标文件
                for (size_t obj_idx = 0; obj_idx < all_objects.size(); obj_idx++) {
                    auto key = std::make_pair(obj_idx, orig_merged_sec);
                    if (section_offsets.find(key) != section_offsets.end() &&
                        section_sizes.find(key) != section_sizes.end()) {
                        size_t start_offset = section_offsets[key];
                        size_t size = section_sizes[key];
                        
                        if (reloc_offset_in_merged >= start_offset && 
                            reloc_offset_in_merged < start_offset + size) {
                            auto local_sym_it = local_symbols_by_obj[obj_idx].find(reloc.symbol);
                            if (local_sym_it != local_symbols_by_obj[obj_idx].end()) {
                                const Symbol& target_sym = local_sym_it->second;
                                
                                // 使用 merged_sec_vaddr 获取符号所在 merged section 的虚拟地址
                                auto vaddr_it = merged_sec_vaddr.find(target_sym.section);
                                if (vaddr_it != merged_sec_vaddr.end()) {
                                    sym_vaddr = base_addr + vaddr_it->second + target_sym.offset;
                                } else {
                                    // fallback: 用前缀匹配
                                    std::string target_out_sec = get_output_section_name(target_sym.section);
                                    sym_vaddr = base_addr + section_vaddr_offsets[target_out_sec] + target_sym.offset;
                                }
                                found = true;
                                break;
                            }
                        }
                    }
                }
                
                if (!found) {
                    throw std::runtime_error("Undefined local symbol: " + reloc.symbol);
                }
            } else {
                // 普通符号 - 现在 global_symbols 已经被正确更新
                auto sym_it = global_symbols.find(reloc.symbol);
                if (sym_it == global_symbols.end() || sym_it->second.type == SymbolType::UNDEFINED) {
                    if (options.shared) {
                        continue;
                    }
                    throw std::runtime_error("Undefined symbol: " + reloc.symbol);
                }
                
                const Symbol& target_sym = sym_it->second;
                if (!target_sym.section.empty()) {
                    // target_sym.section 现在是输出节名（已在步骤6中更新）
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
            
            // 对于.bss节，不写入文件
            if (out_sec_name == ".bss") {
                continue;
            }
            
            // 检查偏移是否在范围内
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
                    reloc_size = 8;
            }
            
            if (reloc.offset + reloc_size > out_sec.data.size()) {
                continue;
            }
            
            // 应用重定位
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
    
    // 8. 对于静态可执行文件，清除已应用的重定位
    if (!options.shared) {
        for (auto& [name, sec] : output_sections) {
            sec.relocs.clear();
        }
    }
    
    // 9. 设置输出对象的节
    output.sections = output_sections;
    output.symbols = output_symbols;
    
    // 10. 设置节头（任务六：正确权限）
    output.shdrs.clear();
    
    for (const auto& sec_name : out_sec_names) {
        SectionHeader shdr;
        shdr.name = sec_name;
        shdr.type = 1;
        
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
    
    // 11. 设置程序头（任务六：正确权限）
    output.phdrs.clear();
    
    for (const auto& sec_name : out_sec_names) {
        ProgramHeader phdr;
        phdr.name = sec_name;
        phdr.vaddr = base_addr + section_vaddr_offsets[sec_name];
        phdr.size = section_mem_sizes[sec_name];
        
        if (sec_name == ".text") {
            phdr.flags = static_cast<uint32_t>(PHF::R) | static_cast<uint32_t>(PHF::X);
        } else if (sec_name == ".rodata") {
            phdr.flags = static_cast<uint32_t>(PHF::R);
        } else {
            phdr.flags = static_cast<uint32_t>(PHF::R) | static_cast<uint32_t>(PHF::W);
        }
        
        output.phdrs.push_back(phdr);
    }
    
    // 12. 设置入口点
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
        if (section_vaddr_offsets.find(".text") != section_vaddr_offsets.end()) {
            output.entry = base_addr + section_vaddr_offsets[".text"];
        } else {
            output.entry = base_addr;
        }
    }
    
    return output;
}