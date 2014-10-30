/**
 	Copyright (C) Oct 17, 2014 xuanhung2401@gmail.com
 */
package test.cloud.google.entity;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import cloud.google.annotation.Annotation.BigQuery_Id;
import cloud.google.annotation.Annotation.BigQuery_Remove;
import cloud.google.util.StringHelper;

/**
 * @author xuanhung2401
 * 
 */
public class Foo {

	@BigQuery_Id
	private String id;
	private String name;
	private String email;
	private int randomInt;
	private double randomDouble;
	private boolean gender;
	private ArrayList<String> interest;
	@BigQuery_Remove
	private String history;
	private Date dob;

	public Foo() {
		super();
		this.id = StringHelper.getId();
		this.name = "";
		this.randomDouble = 2.0;
		this.randomInt = 1;
		this.gender = true;
		this.interest = new ArrayList<String>();
		this.interest.add("Sex");
		this.interest.add("Game");
		this.history = "This is my history !";
		this.dob = Calendar.getInstance().getTime();
	}

	public String getEmail() {
		return email;
	}

	public int getRandomInt() {
		return randomInt;
	}

	public double getRandomDouble() {
		return randomDouble;
	}

	public void setEmail(String email) {
		this.id += email;
		this.email = email;
	}

	public void setRandomInt(int randomInt) {
		this.randomInt = randomInt;
	}

	public void setRandomDouble(double randomDouble) {
		this.randomDouble = randomDouble;
	}

	public String getHistory() {
		return history;
	}

	public void setHistory(String history) {
		this.history = history;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isGender() {
		return gender;
	}

	public void setGender(boolean gender) {
		this.gender = gender;
	}

	public ArrayList<String> getInterest() {
		return interest;
	}

	public void setInterest(ArrayList<String> interest) {
		this.interest = interest;
	}

	public Date getDob() {
		return dob;
	}

	public void setDob(Date dob) {
		this.dob = dob;
	}

}
