local version = ...
local allowed_versions = box.schema.downgrade_versions()

local function is_version_allowed()
        for _, allowed_version in ipairs(allowed_versions) do
                if allowed_version == version then
                        return true
                end
        end
        return false
end

local err
local ok = false

if not is_version_allowed() then
        err =  "Version '" .. version .. "' is not allowed."
        local result = "["
        for i, value in ipairs(allowed_versions) do
                result = result .. tostring(value)
                if i < #allowed_versions then
                        result = result .. ", "
                else
                        result = result .. "]"
                end
        end
        err = err .. "\n\tAllowed versions: " .. result
end

if err == nil then
        ok, err = pcall(box.schema.downgrade, version)
        if ok then
                ok, err = pcall(box.snapshot)
        end
end

return {
        lsn = box.info.lsn,
        iid = box.info.id,
        err = (not ok) and tostring(err) or nil,
}