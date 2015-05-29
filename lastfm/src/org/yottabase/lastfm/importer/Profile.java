package org.yottabase.lastfm.importer;

import java.util.Calendar;

public class Profile {
	
	private String code;
	
	private String gender;
	
	private Integer age;
	
	private String country;
	
	private Calendar registrationDate;

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public Calendar getRegistrationDate() {
		return registrationDate;
	}

	public void setRegistrationDate(Calendar registrationDate) {
		this.registrationDate = registrationDate;
	}

	@Override
	public String toString() {
		return "Profile [code=" + code + "]";
	}
	
}
