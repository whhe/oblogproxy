/**
 * Copyright (c) 2024 OceanBase
 * OceanBase Migration Service LogProxy is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include "model.h"

namespace oceanbase::logproxy {

Model* Void::create() const
{
  return new (std::nothrow) Void;
}

Model* Void::clone() const
{
  return create();
}

List::~List()
{
  clear();
}

List::List() : Model()
{
  _type = ModelTypeEnum::e().LIST;
}

List::List(const List& o) : Model()
{
  _type = ModelTypeEnum::e().LIST;
  assign(&o);
}

List::List(List&& o) noexcept : Model()
{
  _type = ModelTypeEnum::e().LIST;
  assign(&o);
  o.clear();
}

List& List::operator=(const List& o)
{
  _type = ModelTypeEnum::e().LIST;
  assign(&o);
  return *this;
}

List& List::operator=(List&& o) noexcept
{
  _type = ModelTypeEnum::e().LIST;
  assign(&o);
  o.clear();
  return *this;
}

std::string List::debug_str() const
{
  std::string buf = "[";
  for (auto& item : _items) {
    buf += item == nullptr ? "NULL" : item->debug_str();
    buf += ", ";
  }
  if (buf.back() == ' ') {
    buf.pop_back();
    buf.back() = ']';
  } else {
    buf += ']';
  }
  return buf;
}

size_t List::size() const
{
  return _items.size();
}

bool List::empty() const
{
  return _items.empty();
}

void List::clear()
{
  for (auto& item : _items) {
    delete item;
  }
  _items.clear();
}

void List::push_back(Model* model)
{
  if (model == nullptr) {
    _items.push_back(nullptr);
  } else {
    _items.push_back(model);
  }
}

void List::set(size_t idx, Model* model)
{
  if (idx >= _items.size()) {
    return;
  }
  delete _items[idx];
  _items[idx] = model == nullptr ? nullptr : model;
}

const Model* List::operator[](size_t idx) const
{
  return at(idx);
}

// std::vector<Model*>::const_iterator List::begin() const
//{
//   return _items.begin();
// }
//
// std::vector<Model*>::const_iterator List::end() const
//{
//   return _items.end();
// }
std::vector<Model*>& List::get_items()
{
  return _items;
}

Object::~Object()
{
  for (auto& entry : _objects) {
    if (entry.second != nullptr) {
      delete entry.second;
      entry.second = nullptr;
    }
  }
}

std::string Object::debug_str() const
{
  std::string buf = "{";
  for (auto& key : get_plains()) {
    buf += key.first + ": " + key.second->str() + ", ";
  }
  for (auto& entry : get_lists()) {
    buf += entry.first + ": " + entry.second.debug_str() + ", ";
  }
  for (auto& entry : get_objects()) {
    buf += entry.first + ": " + (entry.second == nullptr ? "NULL" : entry.second->debug_str()) + ", ";
  }
  if (buf.back() == ' ') {
    buf.pop_back();
    buf.back() = '}';
  } else {
    buf += '}';
  }
  return buf;
}

Model* Object::get_plain(const std::string& key)
{
  if (!is_plain_member(key)) {
    return nullptr;
  }
  return _plains.find(key)->second;
}

const Model* Object::get_plain(const std::string& key) const
{
  if (!is_plain_member(key)) {
    return nullptr;
  }
  return _plains.find(key)->second;
}

Object* Object::get_object(const std::string& name)
{
  auto entry = _objects.find(name);
  return entry == _objects.end() ? nullptr : entry->second;
}

const Object* Object::get_object(const std::string& name) const
{
  auto entry = _objects.find(name);
  return entry == _objects.end() ? nullptr : entry->second;
}

List& Object::get_list(const std::string& name)
{
  return _lists.at(name);
}

const List& Object::get_list(const std::string& name) const
{
  return _lists.at(name);
}

void Object::init(std::map<std::string, std::string>& options)
{
  for (auto& key : get_plains()) {
    if (options.find(key.first) != options.end()) {
      if (key.second == nullptr) {
        break;
      }
      const char* option = options.find(key.first)->second.c_str();

      switch (key.second->model_type().code) {
        case ModelTypeEnum::CHAR_code:
          ((Plain<char>*)key.second)->set((char)*option);
          break;
        case ModelTypeEnum::INT16_code:
          ((Plain<int16_t>*)key.second)->set((int16_t)atoi(option));
          break;
        case ModelTypeEnum::INT_code:
          ((Plain<int>*)key.second)->set((int32_t)atoi(option));
          break;
        case ModelTypeEnum::INT64_code:
          ((Plain<int64_t>*)key.second)->set((int64_t)atoi(option));
          break;
        case ModelTypeEnum::UINT16_code:
          ((Plain<uint16_t>*)key.second)->set((uint16_t)std::stoul(option));
          break;
        case ModelTypeEnum::UINT32_code:
          ((Plain<uint32_t>*)key.second)->set((uint32_t)std::stoul(option));
          break;
        case ModelTypeEnum::UINT64_code:
          ((Plain<uint64_t>*)key.second)->set((uint64_t)std::stoull(option));
          break;
        case ModelTypeEnum::BOOL_code:
          ((Plain<bool>*)key.second)->set(atoi(option));
          break;
        case ModelTypeEnum::FLOAT_code:
          ((Plain<float>*)key.second)->set(atof(option));
          break;
        case ModelTypeEnum::DOUBLE_code:
          ((Plain<float>*)key.second)->set(atof(option));
          break;
        case ModelTypeEnum::STR_code:
          ((Plain<std::string>*)key.second)->set(option);
          break;
        default:
          break;
      }
    }
  }
}

bool is_plain(const Model* model)
{
  return (model->model_type() != ModelTypeEnum::e().no && model->model_type() != ModelTypeEnum::e().OBJECT &&
          model->model_type() != ModelTypeEnum::e().LIST);
}

bool is_list(const Model* model)
{
  return (model->model_type() == ModelTypeEnum::e().LIST);
}

bool is_object(const Model* model)
{
  return (model->model_type() == ModelTypeEnum::e().OBJECT);
}

static void model_to_json_inner(const Model* model, rapidjson::Value& json, rapidjson::Document& node, bool root = true)
{
  if (model == nullptr) {
    json.SetNull();
    return;
  }

  switch (model->model_type().code) {
    case ModelTypeEnum::CHAR_code: {
      rapidjson::Value value(rapidjson::kNumberType);
      value = ((Plain<char>*)model)->value();
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    case ModelTypeEnum::INT16_code: {
      rapidjson::Value value(rapidjson::kNumberType);
      value = ((Plain<int>*)model)->value();
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    case ModelTypeEnum::INT_code: {
      rapidjson::Value value(rapidjson::kNumberType);
      value = ((Plain<int>*)model)->value();
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    case ModelTypeEnum::INT64_code: {
      rapidjson::Value value(rapidjson::kNumberType);
      value = ((Plain<int64_t>*)model)->value();
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    case ModelTypeEnum::UINT16_code: {
      rapidjson::Value value(rapidjson::kNumberType);
      value = ((Plain<uint16_t>*)model)->value();
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    case ModelTypeEnum::UINT32_code: {
      rapidjson::Value value(rapidjson::kNumberType);
      value = ((Plain<uint32_t>*)model)->value();
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    case ModelTypeEnum::UINT64_code: {
      rapidjson::Value value(rapidjson::kNumberType);
      value = ((Plain<uint64_t>*)model)->value();
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    case ModelTypeEnum::BOOL_code: {
      rapidjson::Value value;
      value = ((Plain<bool>*)model)->value();
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    case ModelTypeEnum::FLOAT_code: {
      rapidjson::Value value(rapidjson::kNumberType);
      value = ((Plain<float>*)model)->value();
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    case ModelTypeEnum::DOUBLE_code: {
      rapidjson::Value value(rapidjson::kNumberType);
      value = ((Plain<double>*)model)->value();
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    case ModelTypeEnum::STR_code: {
      rapidjson::Value value(rapidjson::kStringType);
      value.SetString(((Plain<std::string>*)model)->value().c_str(),
          ((Plain<std::string>*)model)->value().size(),
          node.GetAllocator());
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
      return;
    }
    default:
      break;
  }

  if (model->model_type() == ModelTypeEnum::e().LIST) {
    assert(json.IsArray());
    for (auto& item : ((List*)model)->get_items()) {
      rapidjson::Value item_node = rapidjson::Value(rapidjson::kObjectType);
      model_to_json_inner(item, item_node, node, true);
      json.PushBack(item_node, node.GetAllocator());
    }
    return;
  }

  if (model->model_type() == ModelTypeEnum::e().OBJECT) {
    if (root) {
      for (auto& item : ((Object*)model)->get_plains()) {
        item.second->set_name(item.first);
        model_to_json_inner(item.second, json, node);
      }
      for (auto& item : ((Object*)model)->get_lists()) {
        rapidjson::Value item_node(rapidjson::kArrayType);
        item.second.set_name(item.first);
        model_to_json_inner(&item.second, item_node, node);
        rapidjson::Value key(rapidjson::kStringType);
        key.SetString(item.first.c_str(), item.first.size(), node.GetAllocator());
        json.AddMember(key, item_node, node.GetAllocator());
      }
      for (auto& item : ((Object*)model)->get_objects()) {
        rapidjson::Value item_node(rapidjson::kObjectType);
        model_to_json_inner(item.second, item_node, node);
        rapidjson::Value key(rapidjson::kStringType);
        key.SetString(item.first.c_str(), item.first.size(), node.GetAllocator());
        json.AddMember(key, item_node, node.GetAllocator());
      }
    } else {
      rapidjson::Value value = rapidjson::Value(rapidjson::kObjectType);
      for (auto& item : ((Object*)model)->get_plains()) {
        item.second->set_name(item.first);
        model_to_json_inner(item.second, value, node);
      }
      for (auto& item : ((Object*)model)->get_lists()) {
        rapidjson::Value item_node(rapidjson::kArrayType);
        item.second.set_name(item.first);
        model_to_json_inner(&item.second, item_node, node);
        rapidjson::Value key(rapidjson::kStringType);
        key.SetString(item.first.c_str(), item.first.size(), node.GetAllocator());
        value.AddMember(key, item_node, node.GetAllocator());
      }
      for (auto& item : ((Object*)model)->get_objects()) {
        //        rapidjson::Value item_node(rapidjson::kObjectType);
        model_to_json_inner(item.second, value, node);
        //        rapidjson::Value key(rapidjson::kStringType);
        //        key.SetString(item.first.c_str(), item.first.size(), node.GetAllocator());
        //        value.AddMember(key, item_node, node.GetAllocator());
      }
      rapidjson::Value key(rapidjson::kStringType);
      key.SetString(model->name().c_str(), model->name().size(), node.GetAllocator());
      json.AddMember(key, value, node.GetAllocator());
    }
  }
}

std::string Model::serialize_to_json() const
{
  std::string json_str;
  json_str.clear();
  rapidjson::Document node;
  node.SetObject();
  model_to_json_inner(this, node, node, true);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  node.Accept(writer);
  json_str = buffer.GetString();
  return json_str;
}

rapidjson::Value Model::serialize_to_json_value(rapidjson::Document& document) const
{
  document.SetObject();
  model_to_json_inner(this, document, document, true);
  return document.GetObject();
}

static void json_to_model_inner(const rapidjson::Value& json, Model* model)
{
  if (model == nullptr || json.IsNull()) {
    return;
  }

  switch (model->model_type().code) {
    case ModelTypeEnum::CHAR_code:
      if (json.IsNumber()) {
        ((Plain<char>*)model)->set((char)json.GetInt());
      }
      return;
    case ModelTypeEnum::INT16_code:
      if (json.IsNumber()) {
        ((Plain<int16_t>*)model)->set((int16_t)json.GetInt());
      }
      return;
    case ModelTypeEnum::INT_code:
      if (json.IsNumber()) {
        ((Plain<int>*)model)->set(json.GetInt());
      }
      return;
    case ModelTypeEnum::INT64_code:
      if (json.IsNumber()) {
        ((Plain<int64_t>*)model)->set(json.GetInt64());
      }
      return;
    case ModelTypeEnum::UINT16_code:
      if (json.IsNumber()) {
        ((Plain<uint16_t>*)model)->set((uint16_t)json.GetUint());
      }
      return;
    case ModelTypeEnum::UINT32_code:
      if (json.IsNumber()) {
        ((Plain<uint32_t>*)model)->set(json.GetUint());
      }
      return;
    case ModelTypeEnum::UINT64_code:
      if (json.IsNumber()) {
        ((Plain<uint64_t>*)model)->set(json.GetUint64());
      }
      return;
    case ModelTypeEnum::BOOL_code:
      if (json.IsBool()) {
        ((Plain<bool>*)model)->set(json.GetBool());
      }
      return;
    case ModelTypeEnum::FLOAT_code:
      if (json.IsDouble()) {
        ((Plain<float>*)model)->set(json.GetFloat());
      }
      return;
    case ModelTypeEnum::DOUBLE_code:
      if (json.IsDouble()) {
        ((Plain<float>*)model)->set(json.GetDouble());
      }
      return;
    case ModelTypeEnum::STR_code:
      if (json.IsString()) {
        ((Plain<std::string>*)model)->set(json.GetString());
      }
      return;
    default:
      break;
  }

  if (json.IsArray() && model->model_type() == ModelTypeEnum::e().LIST) {
    List& list = *((List*)model);
    for (size_t i = 0; i < json.Size(); ++i) {
      if (json[i].IsNull()) {
        list.set(i, nullptr);
        continue;
      }
      if (list.empty()) {
        /*!
         * The first element needs to be used to determine the current type. If it is not filled, the list will be
         * considered empty.
         */
        break;
      }

      if (i == 0) {
        json_to_model_inner(json[i], list.at(i));
      } else {
        if (list.size() < i + 1) {
          list.push_back(list[0]->create());
        } else {
          list.set(i, list[0]->create());
        }
        json_to_model_inner(json[i], list.at(i));
      }
    }
    return;
  }

  if (json.IsObject() && model->model_type() == ModelTypeEnum::e().OBJECT) {
    Object* obj = (Object*)model;
    for (auto iterator = json.MemberBegin(); iterator != json.MemberEnd(); ++iterator) {
      std::string json_key = iterator->name.GetString();
      if (obj->is_plain_member(json_key)) {
        json_to_model_inner(json[json_key.c_str()], obj->get_plain(json_key));
      } else if (obj->is_list_member(json_key)) {
        json_to_model_inner(json[json_key.c_str()], &obj->get_list(json_key));
      } else if (obj->is_object_member(json_key)) {
        json_to_model_inner(json[json_key.c_str()], obj->get_object(json_key));
      }
    }
  }
}

int Model::deserialize_from_json(const std::string& value)
{
  if (value.empty()) {
    return OMS_OK;
  }

  rapidjson::Document json;
  json.Parse(value.c_str());
  if (json.HasParseError()) {
    OMS_ERROR("Failed to parse json, error: {}", json.GetErrorOffset());
    return OMS_FAILED;
  }
  json_to_model_inner(json, this);
  return OMS_OK;
}

int Model::deserialize_from_json_value(const rapidjson::Value& value)
{
  if (value.IsNull()) {
    return OMS_OK;
  }

  json_to_model_inner(value, this);
  return OMS_OK;
}
}  // namespace oceanbase::logproxy
