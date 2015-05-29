package org.yottabase.lastfm.importer;

import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

public class UserRecordManager {
	
	public User getUserFromLine(String line) {
		
		List<String> values = Arrays.asList( line.split("\t") );
		Iterator<String> iter = values.iterator();
		
		String code = (iter.hasNext()) ? iter.next() : "";
		String gender = (iter.hasNext()) ? iter.next() : "";
		String age = (iter.hasNext()) ? iter.next() : "";
		String country = (iter.hasNext()) ? iter.next() : "";
//		String signupDate = parsered[4]; //TODO
		
		User user = new User();
		user.setCode(code);
		user.setGender( (gender.equals("")) ? null : gender );
		user.setAge( (age.equals("")) ? null : Integer.parseInt(age) );
		user.setCountry( (country.equals("")) ? null : country );
		user.setSignupDate( new GregorianCalendar() );
		
		return user;
	}

}
