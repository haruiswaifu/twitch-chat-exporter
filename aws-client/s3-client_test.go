package aws_client

import "testing"

func Test_createPartitionQueryTemplate(t *testing.T) {
	type args struct {
		channels []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "basic",
			args: args{
				channels: []string{"jinnytty", "haruiswaifu"},
			},
			want: "something",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createPartitionQueryTemplate(tt.args.channels); got != tt.want {
				t.Errorf("createPartitionQueryTemplate() = %v, want %v", got, tt.want)
			}
		})
	}
}
