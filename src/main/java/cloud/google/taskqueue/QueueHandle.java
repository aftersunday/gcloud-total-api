/**
 	Copyright (C) Oct 7, 2014 xuanhung2401@gmail.com
 */
package cloud.google.taskqueue;

import cloud.google.util.ConnectionService;

/**
 * @author xuanhung2401
 * 
 */
public class QueueHandle {
	public static void main(String[] args) {
		ConnectionService.connect("http://httpsns.appspot.com/api?name=build-again").body("sk8.vn \n youtube.com \n google.com").post();
	}
}
