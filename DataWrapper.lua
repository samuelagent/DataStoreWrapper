--[[
author: samuelagent

This wrapper makes working with DataStoreService easier. It follows a similar style to DataStore in that referenced data is cached and saved with backups through
OrderedDataStores. Use case also extends beyond saving player data, methods are provided for handling these caches and saves.


Function > DataWrapper.GetData(Key: Player | String | Number, [Optional] Scope: String | Number)
	Creates and returns a DataStore object for the referenced Key and Scope. If the provided Key is not a Player Instance, UserId of a player current in the server,
	or arbitrary data that is not considered part of PlayerData, then you must handle the saving and disposal of these objects manually.
	
Function > DataObject:Get(DefaultValue: AnyDataStorableValue)
	Returns the cached data of the object. If there is no cached data then it will yield until data is retrieved from its datastore. If the DefaultValue is a table
	then the cached data will be filled with any keys that are missing in relation to the default table.

Function > DataObject:Increment(Amount: Number)
	Increments the cached data of the object by the given amount.
	
Function > DataObject:Set(Value: AnyDataStorableValue)
	Sets the object's cached value to the given Value.
	
Function > DataObject:Save()
	Saves the object's cached value to DataStore. Yields until data is saved or the attempt fails.
	
Function > DataObject:Remove()
	Removes all references of the given data object without saving the cached data. This is called automatically if the DataObject is of a currently present player
	within the server, otherwise it is never called internally. 


Quick Note *

Use as few DataObjects as possible in order to reduce throttling and data loss. This means saving large amounts of data under single DataObjects rather than over
using multiple scopes, datastores, etc. You can use a dictionary to further sort your own data rather than becoming over-dependant on the datastore scopes, only
utilize new DataObjects when necessary.

]]


--// Services
local DataStoreService = game:GetService("DataStoreService")
local RunService = game:GetService("RunService")
local Players = game:GetService("Players")

--// Runtime

local CachedPlayerObjects = {}
local CurrentGameUserIds = {}
local CachedDataObjects = {}

--// Auxillary

local GlobalDataStore = DataStoreService:GetGlobalDataStore()

local function CloneTable(Original)
	local NewTable = {}
	for i, v in pairs(Original) do
		if type(v) == "table" then
			v = CloneTable(v)
		end
		NewTable[i] = v
	end
	return NewTable
end

local function ClearObject(Object)
	local Tables = {}
	for _, v in pairs(Object) do
		if type(v) == "table" then
			table.insert(Tables, v)
			ClearObject(v)
		end
	end
	for _, v in pairs(Tables) do
		table.clear(v)
	end
	table.clear(Object)
end

local function PlayerOfUserIdInServer(UserId)
	if CurrentGameUserIds[UserId] then return true end
	for _, Player in pairs(Players:GetPlayers()) do
		CurrentGameUserIds[Player.UserId] = true
		if Player.UserId == UserId then return true end
	end
end

--// Classes

local DataWrapper = {}

DataWrapper.__index = DataWrapper

DataWrapper.AutoDeloadPlayerCaches = true
DataWrapper.SetCacheToFirstDefault = false

DataWrapper.NoDataHolder = "N/A" -- How to reference retrieved data that does not exist in stead of nil
DataWrapper.SaveInStudio = true
DataWrapper.SaveOnPlayerLeave = true

DataWrapper.GlobalScopeKey = "GlobalScopeKey"
DataWrapper.PlayerKeyPrefix = "User"

DataWrapper.AttemptDelay = 0.5
DataWrapper.AttemptCount = 3

DataWrapper.LoadDataInstantly = true -- Whether to load data immediately or after first :Get() call

local DataClass  = {}
DataClass.__index = DataClass

local OrderedDataStores = {}
OrderedDataStores.__index = OrderedDataStores

--// DataWrapper (Module)

function DataWrapper.GetObjectCache(Key, Scope)
	if not CachedDataObjects[Key] then CachedDataObjects[Key] = {} end
	return CachedDataObjects[Key][Scope or DataWrapper.GlobalScopeKey]
end

function DataWrapper.PlayerKeyFromUserId(UserId)
	return DataWrapper.PlayerKeyPrefix .. UserId
end

function DataWrapper.GetDataStoreKey(Data: String | UserId | Player)
	local Success, Error = pcall(function()
		Players:GetNameFromUserIdAsync(tonumber(Data))
	end)
	return Success and DataWrapper.PlayerKeyFromUserId(Data) or (typeof(Data) == "Instance" and Data:IsA("Player")) and DataWrapper.PlayerKeyFromUserId(Data.UserId) or Data
end

function DataWrapper.GetRawData(Key, Scope)
	local DataStore = Scope and DataStoreService:GetDataStore(Key, Scope or "") or GlobalDataStore
	local Success, Error
	local Data

	for count = 1, DataWrapper.AttemptCount do
		Success, Error = pcall(function()
			Data = DataStore:GetAsync(Key, Scope)
		end)
		if Success then break else
			task.wait(DataWrapper.AttemptDelay)
		end
	end

	return Data, DataStore, Success
end

function DataWrapper.GetData(Name, Scope, DefaultData)
	local Key = DataWrapper.GetDataStoreKey(Name)
	local DataObject = DataWrapper.GetObjectCache(Key, Scope)

	if DataObject then
		return DataObject
	else
		return DataClass.New(Key, Scope)
	end
end

function DataWrapper.GDPR(UserId)
	local Key = DataWrapper.GetDataStoreKey(UserId)
	
end

--// DataClass (Data Object)

function DataClass:GetRaw()
	local Success, Data = self.SavingMethod:GetOrderedDataStore()
	return Data
end

function DataClass:Get(DefaultValue)
	local Data = (self.Value ~= DataWrapper.NoDataHolder and self.Value) or self:GetRaw()
	
	if type(Data) == "table" and type(DefaultValue) == "table" then
		for i, v in pairs(DefaultValue) do
			if Data[i] == nil then
				Data[i] = v
			end
		end
	end
	
	if DataWrapper.SetCacheToFirstDefault and Data == DataWrapper.NoDataHolder then
		self.Value = Data
	end
	
	return Data ~= DataWrapper.NoDataHolder and Data or DefaultValue
end

function DataClass:Set(Value)
	self.Value = Value
	self.Changed = true
end

function DataClass:Save()
	if not self.Changed then warn("DataStore did not save because: DATA WAS NEVER UPDATED") return end
	self.SavingMethod:SetOrderedDataStore(self.Value)
	self.Changed = false
end

function DataClass:Increment(Number)
	assert(type(Number) == "number", "Number expected, got " .. typeof(Number))
	self.Value += Number
	self.Changed = true
end

function DataClass:Remove()
	if self.UserId then CachedPlayerObjects[self.UserId] = nil end
	
	if CachedDataObjects[self.Key] then
		CachedDataObjects[self.Key][self.Scope or DataWrapper.GlobalScopeKey] = nil
	end
	ClearObject(self)
end

function DataClass.New(Key, Scope)
	local NewDataObject = {
		["SavingMethod"] = OrderedDataStores.New(Key, Scope),
		["Value"] = DataWrapper.NoDataHolder,
		["Scope"] = Scope,
		["Key"] = Key,
		["UserId"] = nil,
		["Changed"] = false,
	}
	
	setmetatable(NewDataObject, DataClass)
	
	if DataWrapper.LoadDataInstantly then
		NewDataObject.Value = NewDataObject:GetRaw()	
	end
	
	if not CachedDataObjects[Key] then CachedDataObjects[Key] = {} end
	
	local PlayerKeyPrefix = string.sub(Key, 1, #DataWrapper.PlayerKeyPrefix)
	local PlayerKeyUserId = tonumber(string.sub(Key, #DataWrapper.PlayerKeyPrefix + 1, #Key))
	
	if PlayerKeyPrefix == DataWrapper.PlayerKeyPrefix then
		NewDataObject.UserId = PlayerKeyUserId
		if PlayerOfUserIdInServer(PlayerKeyUserId) then
			CachedPlayerObjects[PlayerKeyUserId] = NewDataObject
		end
	end
	
	return NewDataObject
end

--// OrderedDataStores (Saving Method)

function OrderedDataStores:GetOrderedDataStore()
	local Success, Error
	local Data = nil
	for count = 1, DataWrapper.AttemptCount do
		Success, Error = pcall(function()
			Data = self.OrderedDataStore:GetSortedAsync(false, 1):GetCurrentPage()[1]
		end)
		if Success then break else
			task.wait(DataWrapper.AttemptDelay)
		end
	end
	
	self.MostRecentKey = Data and Data.key or 0

	if not Success then
		return false, Error
	elseif Data then
		for count = 1, DataWrapper.AttemptCount do
			Success, Error = pcall(function()
				Data = self.DataStore:GetAsync(self.MostRecentKey)
			end)
			if Success then break else
				task.wait(DataWrapper.AttemptDelay)
			end
		end
		
		return Success and true, Data
	else
		return true, nil
	end
end

function OrderedDataStores:SetOrderedDataStore(Value)
	local Key = (self.MostRecentKey or 0) + 1
	
	local Success, Error

	for count = 1, DataWrapper.AttemptCount do
		Success, Error = pcall(function()
			self.DataStore:SetAsync(Key, Value)
		end)
		if Success then break else
			task.wait(DataWrapper.AttemptDelay)
		end
	end

	if not Success then
		warn("DataStore did not save because: FAILED TO SAVE NEW DATASTORE")
		return false, Error
	end
	
	for count = 1, DataWrapper.AttemptCount do
		Success, Error = pcall(function()
			self.OrderedDataStore:SetAsync(Key, os.time())
		end)
		if Success then break else
			task.wait(DataWrapper.AttemptDelay)
		end
	end

	if not Success then
		warn("DataStore did not save because: FAILED TO SAVE NEW KEY INDEX")
		return false, Error
	else
		self.MostRecentKey = Key
	end

	return true
end

function OrderedDataStores.New(Key, Scope)
	local Data = {
		["DataStore"] = Scope and DataStoreService:GetDataStore(Key, Scope or "") or GlobalDataStore,
		["OrderedDataStore"] = DataStoreService:GetOrderedDataStore(Key, Scope)
	}
	
	return setmetatable(Data, OrderedDataStores)
end

--// Events

Players.PlayerRemoving:Connect(function(Player)
	local PlayerObject = CachedPlayerObjects[Player.UserId]
	if PlayerObject then
		if DataWrapper.SaveOnPlayerLeave and PlayerObject.Changed then CachedPlayerObjects[Player.UserId]:Save() end
		PlayerObject:Remove()
	end
	CachedPlayerObjects[Player.UserId] = nil
	CurrentGameUserIds[Player.UserId] = nil
end)

return setmetatable(DataWrapper, DataWrapper)
