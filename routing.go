package golangNeo4jBoltDriver

import (
	"fmt"
	"github.com/mindstand/golang-neo4j-bolt-driver/errors"
	"strings"
)

//use this in testing https://github.com/graphaware/neo4j-casual-cluster-quickstart/blob/master/docker-compose.yml
//refer here https://github.com/cleishm/libneo4j-client/issues/26#issuecomment-388623799

const clusterOverview = "call dbms.cluster.overview()"

type dbAction int

const (
	Read  dbAction = 0
	Write dbAction = 1
)

type neoNodeType int

func infoFromRoleString(s string) (neoNodeType, dbAction) {
	adjustedString := strings.ToLower(s)

	switch adjustedString {
	case "leader":
		return Leader, Write
	case "follower":
		return Follower, Read
	case "read_replica":
		return ReadReplica, Read
	case "write_replica":
		return WriteReplica, Write
	default:
		return -1, -1
	}
}

const (
	Leader       neoNodeType = 0
	Follower     neoNodeType = 1
	ReadReplica  neoNodeType = 2
	WriteReplica neoNodeType = 3
)

type neoNodeConfig struct {
	Id        string
	Addresses []string
	Database  string
	Groups    []string

	Action dbAction
	Type   neoNodeType
}

type clusterConnectionConfig struct {
	Leaders       []neoNodeConfig
	Followers     []neoNodeConfig
	ReadReplicas  []neoNodeConfig
	WriteReplicas []neoNodeConfig
}

func getClusterInfo(conn Conn) (*clusterConnectionConfig, error) {
	if conn == nil {
		return nil, errors.New("bolt connection can not be nil")
	}

	res, err := conn.QueryNeo(clusterOverview, nil)
	if err != nil {
		return nil, err
	}

	defer res.Close()

	rows, _, err := res.All()
	if err != nil {
		return nil, err
	}

	clusterConfig := clusterConnectionConfig{
		Leaders:       []neoNodeConfig{},
		Followers:     []neoNodeConfig{},
		ReadReplicas:  []neoNodeConfig{},
		WriteReplicas: []neoNodeConfig{},
	}

	for _, row := range rows {
		id, addresses, role, groups, database, err := parseRow(row)
		if err != nil {
			return nil, err
		}

		nodeType, action := infoFromRoleString(role)

		node := neoNodeConfig{
			Id:        id,
			Addresses: addresses,
			Database:  database,
			Groups:    groups,
			Action:    action,
			Type:      nodeType,
		}

		switch nodeType {
		case Leader:
			clusterConfig.Leaders = append(clusterConfig.Leaders, node)
			break
		case Follower:
			clusterConfig.Followers = append(clusterConfig.Followers, node)
			break
		case ReadReplica:
			clusterConfig.ReadReplicas = append(clusterConfig.ReadReplicas, node)
			break
		case WriteReplica:
			clusterConfig.WriteReplicas = append(clusterConfig.WriteReplicas, node)
			break
		default:
			//todo figure out any other types, for now just make it a read replica
			clusterConfig.ReadReplicas = append(clusterConfig.ReadReplicas, node)
		}
	}

	return &clusterConfig, nil
}

/*
	[0]   [1]   [2]   [3]     [4]
	id  address role groups database
*/
func parseRow(row []interface{}) (id string, addresses []string, role string, groups []string, database string, err error) {
	//validate length is correct
	rowLen := len(row)
	if rowLen != 5 {
		return "", nil, "", nil, "", fmt.Errorf("invalid number of rows for query `%s`. %v != 5", clusterOverview, rowLen)
	}

	var ok bool

	id, ok = row[0].(string)
	if !ok {
		return "", nil, "", nil, "", errors.New("unable to parse id into string")
	}

	addresses, err = convertInterfaceToStringArr(row[1])
	if err != nil {
		return "", nil, "", nil, "", errors.New("unable to parse addresses into []string")
	}

	role, ok = row[2].(string)
	if !ok {
		return "", nil, "", nil, "", errors.New("unable to parse role into string")
	}

	groups, err = convertInterfaceToStringArr(row[3])
	if err != nil {
		return "", nil, "", nil, "", errors.New("unable to parse groups into []string")
	}

	database, ok = row[4].(string)
	if !ok {
		return "", nil, "", nil, "", errors.New("unable to parse database into string")
	}
	return id, addresses, role, groups, database, nil
}

func convertInterfaceToStringArr(i interface{}) ([]string, error) {
	if i == nil {
		return nil, errors.New("iarr cannot be nil")
	}

	iarr, ok := i.([]interface{})
	if !ok {
		return nil, errors.New("unable to cast to []interface{}")
	}

	if len(iarr) == 0 {
		return []string{}, nil
	}

	arr := make([]string, len(iarr), cap(iarr))

	for k, v := range iarr {
		arr[k], ok = v.(string)
		if !ok {
			return nil, errors.New("unable to parse interface{} to string")
		}
	}

	return arr, nil
}
