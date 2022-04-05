package com.autograder.sqlite.controller;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.autograder.sqlite.service.SqliteService;

@RestController
@CrossOrigin
@RequestMapping("/sqlite")
public class SqliteGraderController {
	private SqliteService sqliteService = new SqliteService();
	
	/*@GetMapping("/getAllTableNames")
	public List<String> getAllTableNames() throws Exception {
		return sqliteUtility.getAllTableNames();
	}*/
}