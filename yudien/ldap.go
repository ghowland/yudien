package yudien

import (
	"fmt"
	. "github.com/ghowland/yudien/yudienutil"
	"gopkg.in/ldap.v2"
	"os/user"
	"strconv"
	"strings"
)

type LdapUser struct {
	IsAuthenticated bool
	Error           string

	Username string
	Groups   []string

	FirstName string
	FullName  string
	Email     string

	HomeDir string
	Uid     int
}

func LdapLogin(username string, password string) LdapUser {
	// Set up return value, we can return any time
	ldap_user := LdapUser{}
	ldap_user.Username = username

	// Get all LDAP auth from config file...  JSON is fine...

	usr, _ := user.Current()
	homedir := usr.HomeDir

	server_port := ReadPathData(fmt.Sprintf("%s/secure/ldap_connect_port.txt", homedir)) // Should contain contents, no newlines: host.domain.com:389
	server_port = strings.Trim(server_port, " \n")

	fmt.Printf("LDAP: %s\n", server_port)

	l, err := ldap.Dial("tcp", server_port)
	if err != nil {
		ldap_user.IsAuthenticated = false
		ldap_user.Error = err.Error()
		return ldap_user
	}
	defer l.Close()

	fmt.Printf("Dial complete\n")

	ldap_password := ReadPathData(fmt.Sprintf("%s/secure/notcleartextpasswords.txt", homedir)) // Should contain exact password, no newlines.
	ldap_password = strings.Trim(ldap_password, " \n")

	sbr := ldap.SimpleBindRequest{}

	ldap_userconnect := ReadPathData(fmt.Sprintf("%s/secure/ldap_userconnectstring.txt", homedir)) // Should contain connection string, no newlines: "dc=example,dc=com"
	ldap_userconnect = strings.Trim(ldap_userconnect, " \n")

	sbr.Username = ldap_userconnect
	sbr.Password = ldap_password
	_, err = l.SimpleBind(&sbr)
	if err != nil {
		ldap_user.IsAuthenticated = false
		ldap_user.Error = err.Error()
		return ldap_user
	}

	fmt.Printf("Bind complete\n")

	// Get User account

	filter := fmt.Sprintf("(uid=%s)", username)
	fmt.Printf("Filter: %s\n", filter)

	//TODO(g): Get these from JSON or something?  Not sure...  Probably JSON.  This is all ghetto, but it keeps things mostly anonymous and flexible
	attributes := []string{"cn", "gidNumber", "givenName", "homeDirectory", "loginShell", "mail", "sn", "uid", "uidNumber", "userPassword"}

	ldap_usersearch := ReadPathData(fmt.Sprintf("%s/secure/ldap_usersearch.txt", homedir)) // Should contain connection string, no newlines: "dc=example,dc=com"
	ldap_usersearch = strings.Trim(ldap_usersearch, " \n")

	sr := ldap.NewSearchRequest(ldap_usersearch, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false, filter, attributes, nil)

	user_result, err := l.Search(sr)
	if err != nil {
		ldap_user.IsAuthenticated = false
		ldap_user.Error = err.Error()
		return ldap_user
	}

	fmt.Printf("User Search complete: %d\n", len(user_result.Entries))

	for count, first := range user_result.Entries {

		//username = first.GetAttributeValue("sn")

		fmt.Printf("User %d: %s\n", count, first.DN)

		// Populate the result
		ldap_user.FirstName = first.GetAttributeValue("givenName")
		ldap_user.Email = first.GetAttributeValue("mail")
		ldap_user.FullName = first.GetAttributeValue("cn")
		ldap_user.Uid, _ = strconv.Atoi(first.GetAttributeValue("uidNumber"))

		for _, attr := range attributes {
			fmt.Printf("    %s == %v\n", attr, first.GetAttributeValue(attr))
		}

	}

	// Get group info for User

	filter = "(cn=*)"
	fmt.Printf("Group Filter: %s\n", filter)

	//TODO(g): Get these from JSON or something?  Not sure...  Probably JSON.  This is all ghetto, but it keeps things mostly anonymous and flexible
	attributes = []string{"cn", "gidNumber", "memberUid"}

	ldap_groupsearch := ReadPathData(fmt.Sprintf("%s/secure/ldap_groupsearch.txt", homedir)) // Should contain connection string, no newlines: "ou=groups,dc=example,dc=com"
	ldap_groupsearch = strings.Trim(ldap_groupsearch, " \n")

	sr = ldap.NewSearchRequest(ldap_groupsearch, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false, filter, attributes, nil)

	group_result, err := l.Search(sr)
	if err != nil {
		ldap_user.IsAuthenticated = false
		ldap_user.Error = err.Error()
		return ldap_user
	}

	fmt.Printf("Group Search complete: %d\n", len(group_result.Entries))

	user_groups := make([]string, 0)

	for count, first := range group_result.Entries {

		fmt.Printf("Group %d: %s\n", count, first.DN)

		group := first.GetAttributeValue("cn")
		group_users := first.GetAttributeValues("memberUid")

		for _, group_user := range group_users {
			if group_user == username {
				user_groups = append(user_groups, group)
			}
		}
	}

	fmt.Printf("User: %s  Groups: %v\n", username, user_groups)

	// Testing password
	err = l.Bind(fmt.Sprintf("uid=%s,%s", username, ldap_usersearch), password)
	if err != nil {
		ldap_user.IsAuthenticated = false
		ldap_user.Error = err.Error()
		return ldap_user
	}

	fmt.Printf("Password is correct\n")

	//TODO(g): make a struct and pack this data into it:  LdapUser{}
	ldap_user.IsAuthenticated = true
	ldap_user.Groups = user_groups

	return ldap_user
}
