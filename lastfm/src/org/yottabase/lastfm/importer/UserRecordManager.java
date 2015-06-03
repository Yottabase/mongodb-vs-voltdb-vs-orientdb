package org.yottabase.lastfm.importer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class UserRecordManager {
	
	public User getUserFromLine(String line) {
		
		SimpleDateFormat formatter = new SimpleDateFormat("MMM dd, yyyy", Locale.ENGLISH);
		
		List<String> values = Arrays.asList( line.split("\t") );
		Iterator<String> iter = values.iterator();
		
		String code = (iter.hasNext()) ? iter.next() : "";
		String gender = (iter.hasNext()) ? iter.next() : "";
		String age = (iter.hasNext()) ? iter.next() : "";
		String country = (iter.hasNext()) ? iter.next() : "";
		String signupDate = (iter.hasNext()) ? iter.next() : "";
		
		User user = new User();
		user.setCode(code);
		user.setGender( (gender.equals("")) ? null : gender );
		user.setAge( (age.equals("")) ? null : Integer.parseInt(age) );
		user.setCountry( (country.equals("")) ? null : country );
		
		if(! signupDate.equals("")){
			try {
				Calendar calendar = new GregorianCalendar();
				calendar.setTime(formatter.parse(signupDate));
				user.setSignupDate(calendar);
			} catch (ParseException e) {
				e.printStackTrace();
			}	
		}
		
		return user;
	}

}
