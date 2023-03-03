function read_file(){
    var file= document.getElementById('f_name').innerText
    console.log("file_name==",file)
    $.ajax({
        type: "GET",
        url: "/file_read/",
        data: {
            file: file
        },
        success: function (data) {},
        error: function (response) {
          Swal.fire({
            icon: "error",
            title: "Oops...",
            text: "Something went wrong!",
          });
        },
      });
}