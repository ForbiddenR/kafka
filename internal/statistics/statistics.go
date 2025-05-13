package statistics

type Statistics struct {
	Timestamp int64 `json:"timestamp"`
	Protocol string `json:"protocolType"`
	Hostname string `json:"hostname"`
	EquipmentSn string `json:"equipmentSn"`
}