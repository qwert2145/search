package com.my.model;

import java.io.Serializable;
import java.util.Date;

public class Question implements Serializable {
	private static final long serialVersionUID = 1L;

	private long id;

	private int answers;

	private String content;

	private Date creationDate;

	private Date lastAnswerDate;

	private Date lastViewDate;

	private int status;

	private String title;

	private long typeId;

	private long userId;

	private int views;

	private int awards;

	private boolean anonymous;

	private String addition;
	
	private int sames;

	public Question() {
	}

	public long getId() {
		return this.id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public int getAnswers() {
		return this.answers;
	}

	public void setAnswers(int answers) {
		this.answers = answers;
	}

	public String getContent() {
		return this.content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Date getCreationDate() {
		return this.creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public Date getLastAnswerDate() {
		return this.lastAnswerDate;
	}

	public void setLastAnswerDate(Date lastAnswerDate) {
		this.lastAnswerDate = lastAnswerDate;
	}

	public Date getLastViewDate() {
		return this.lastViewDate;
	}

	public void setLastViewDate(Date lastViewDate) {
		this.lastViewDate = lastViewDate;
	}

	public int getStatus() {
		return this.status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getTitle() {
		return this.title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public long getTypeId() {
		return this.typeId;
	}

	public void setTypeId(long typeId) {
		this.typeId = typeId;
	}

	public long getUserId() {
		return this.userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public int getViews() {
		return this.views;
	}

	public void setViews(int views) {
		this.views = views;
	}

	public int getAwards() {
		return awards;
	}

	public void setAwards(int awards) {
		this.awards = awards;
	}

	public String getAddition() {
		return addition;
	}

	public void setAddition(String addition) {
		this.addition = addition;
	}

	public boolean isAnonymous() {
		return anonymous;
	}

	public void setAnonymous(boolean anonymous) {
		this.anonymous = anonymous;
	}

	public int getSames() {
		return sames;
	}

	public void setSames(int sames) {
		this.sames = sames;
	}

}