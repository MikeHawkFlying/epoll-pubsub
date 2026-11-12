-- Wireshark dissector for the pubsub wire protocol.
--   wireshark -X lua_script:tools/pubsub.lua capture.pcap
-- Decodes TCP port 7400; use "Decode As..." for other ports.

local proto = Proto("pubsub", "PubSub Protocol")

local types = {
    [0x01] = "SUB", [0x02] = "UNSUB", [0x03] = "PUB", [0x04] = "PING", [0x05] = "STATS",
    [0x81] = "OK", [0x82] = "MSG", [0x83] = "PONG", [0x84] = "STATS_REPLY", [0x85] = "ERR",
}

local f_len = ProtoField.uint32("pubsub.len", "Body length", base.DEC)
local f_type = ProtoField.uint8("pubsub.type", "Type", base.HEX, types)
local f_topic = ProtoField.string("pubsub.topic", "Topic")
local f_payload = ProtoField.bytes("pubsub.payload", "Payload")
local f_code = ProtoField.uint8("pubsub.error", "Error code", base.DEC)
proto.fields = { f_len, f_type, f_topic, f_payload, f_code }

local function frame_len(tvb, pinfo, offset)
    return tvb(offset, 4):uint() + 4
end

local function dissect_frame(tvb, pinfo, tree)
    local len = tvb(0, 4):uint()
    if len == 0 or tvb:len() < 5 then
        -- the server rejects these, but they can still show up on the wire
        pinfo.cols.protocol = "PUBSUB"
        pinfo.cols.info:append(" EMPTY")
        tree:add(proto, tvb(), "PubSub empty frame"):add(f_len, tvb(0, 4))
        return tvb:len()
    end
    local t = tvb(4, 1):uint()
    local name = types[t] or string.format("0x%02x", t)

    pinfo.cols.protocol = "PUBSUB"
    pinfo.cols.info:append(" " .. name)

    local sub = tree:add(proto, tvb(), "PubSub " .. name)
    sub:add(f_len, tvb(0, 4))
    sub:add(f_type, tvb(4, 1))
    if len <= 1 then
        return tvb:len()
    end

    if t == 0x01 or t == 0x02 then
        sub:add(f_topic, tvb(5, len - 1))
    elseif t == 0x03 or t == 0x82 then
        local tl = tvb(5, 1):uint()
        if 1 + 1 + tl <= len then
            if tl > 0 then
                sub:add(f_topic, tvb(6, tl))
            end
            if len - 2 - tl > 0 then
                sub:add(f_payload, tvb(6 + tl, len - 2 - tl))
            end
        end
    elseif t == 0x85 then
        sub:add(f_code, tvb(5, 1))
        if len > 2 then
            sub:add(f_payload, tvb(6, len - 2))
        end
    else
        sub:add(f_payload, tvb(5, len - 1))
    end
    return tvb:len()
end

function proto.dissector(tvb, pinfo, tree)
    dissect_tcp_pdus(tvb, tree, 4, frame_len, dissect_frame)
end

DissectorTable.get("tcp.port"):add(7400, proto)
